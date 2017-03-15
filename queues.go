/*
  Copyright 2016 Jens Schumann <schumajs@gmail.com>

  Use of this source code is governed by the MIT license that can be found in
  the LICENSE file.
*/

package queues

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/gob"
	"reflect"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/schumajs/go-errors"
)

type Queue interface {
	Enqueue(v interface{}) error
	Dequeue(peek ...bool) (interface{}, bool, error)
	Close() error
}

type chanQueue struct {
	elements chan interface{}
}

func NewChanQueue(capacity ...int) (Queue, error) {
	if len(capacity) == 0 {
		capacity = append(capacity, 0)
	}

	q := &chanQueue{}

	q.elements = make(chan interface{}, capacity[0])

	return q, nil
}

func (q *chanQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
	if len(peek) != 0 {
		return nil, false, errors.New("peek not supported")
	}

	v, open := <-q.elements

	return v, !open, nil
}

func (q *chanQueue) Enqueue(v interface{}) error {
	q.elements <- v

	return nil
}

func (q *chanQueue) Close() error {
	close(q.elements)

	return nil
}

type listQueue struct {
	elements *list.List
	lock     *sync.Mutex
	cond     *sync.Cond
	closed   bool
}

func NewListQueue() (Queue, error) {
	q := &listQueue{}

	q.elements = list.New()

	q.lock = &sync.Mutex{}

	q.cond = sync.NewCond(q.lock)

	return q, nil
}

func (q *listQueue) length() int {
	return q.elements.Len()
}

func (q *listQueue) dequeue(peek bool) interface{} {
	listElement := q.elements.Front()

	element := listElement.Value

	if !peek {
		q.elements.Remove(listElement)
	}

	return element
}

func (q *listQueue) enqueue(v interface{}) {
	q.elements.PushBack(v)
}

func (q *listQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
	if len(peek) == 0 {
		peek = append(peek, false)
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	for !(q.length() > 0 || q.closed) {
		q.cond.Wait()
	}

	switch {
	case q.length() > 0:
		return q.dequeue(peek[0]), false, nil
	case q.closed:
		return nil, true, nil
	}

	return nil, false, nil
}

func (q *listQueue) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.enqueue(v)

	q.cond.Signal()

	return nil
}

func (q *listQueue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.closed = true

	q.cond.Signal()

	return nil
}

type boltDbQueue struct {
	db           *bolt.DB
	elementTypes map[string]reflect.Type
	lock         *sync.Mutex
	cond         *sync.Cond
	closed       bool
}

func NewBoltDbQueue(db *bolt.DB, elementTypes []interface{}) (Queue, error) {
	q := &boltDbQueue{}

	q.db = db

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("queue"))
		if err != nil {
			return errors.Wrap(err)
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err)
	}

	q.elementTypes = map[string]reflect.Type{}

	for _, v := range elementTypes {
		q.elementTypes[reflect.ValueOf(v).Type().Name()] = reflect.ValueOf(v).Type()
	}

	q.lock = &sync.Mutex{}

	q.cond = sync.NewCond(q.lock)

	return q, nil
}

func (q *boltDbQueue) length() (int, error) {
	var length int

	err := q.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("queue"))

		stats := bucket.Stats()

		length = stats.KeyN

		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err)
	}

	return length, nil
}

func (q *boltDbQueue) dequeue(peek bool) (interface{}, error) {
	var v interface{}

	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("queue"))

		cursor := bucket.Cursor()

		key, value := cursor.First()

		if key == nil {
			return nil
		}

		buffer := bytes.NewReader(value)

		decoder := gob.NewDecoder(buffer)

		var typeName string

		err := decoder.Decode(&typeName)
		if err != nil {
			return errors.Wrap(err)
		}

		reflectType, ok := q.elementTypes[typeName]
		if !ok {
			return errors.New("invalid element type")
		}

		reflectValue := reflect.Indirect(reflect.New(reflectType))

		err = decoder.DecodeValue(reflectValue)
		if err != nil {
			return errors.Wrap(err)
		}

		v = reflectValue.Interface()

		if !peek {
			err = cursor.Delete()
			if err != nil {
				return errors.Wrap(err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err)
	}

	return v, nil
}

func (q *boltDbQueue) enqueue(v interface{}) error {
	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("queue"))

		nextSeq, _ := bucket.NextSequence()

		key := make([]byte, 8)

		binary.BigEndian.PutUint64(key, nextSeq)

		var buffer bytes.Buffer

		encoder := gob.NewEncoder(&buffer)

		typeName := reflect.TypeOf(v).Name()

		err := encoder.Encode(&typeName)
		if err != nil {
			return errors.Wrap(err)
		}

		err = encoder.Encode(v)
		if err != nil {
			return errors.Wrap(err)
		}

		err = bucket.Put(key, buffer.Bytes())
		if err != nil {
			return errors.Wrap(err)
		}

		return nil
	})
	if err != nil {
		return errors.Wrap(err)
	}

	return nil
}

func (q *boltDbQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
	if len(peek) == 0 {
		peek = append(peek, false)
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	length, err := q.length()
	if err != nil {
		return nil, false, errors.Wrap(err)
	}

	for !(length > 0 || q.closed) {
		q.cond.Wait()

		length, err = q.length()
		if err != nil {
			return nil, false, errors.Wrap(err)
		}
	}

	switch {
	case length > 0:
		v, err := q.dequeue(peek[0])
		if err != nil {
			return nil, false, errors.Wrap(err)
		}

		return v, false, nil
	case q.closed:
		return nil, true, nil
	}

	return nil, false, nil
}

func (q *boltDbQueue) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	err := q.enqueue(v)
	if err != nil {
		return errors.Wrap(err)
	}

	q.cond.Signal()

	return nil
}

func (q *boltDbQueue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.closed = true

	q.cond.Signal()

	return nil
}

type joinedQueue struct {
	inQs []Queue
	outQ Queue
	errC chan error
}

func newJoinedQueue(inQs ...Queue) (Queue, error) {
	q := &joinedQueue{}

	q.inQs = inQs

	outQ, err := NewChanQueue()
	if err != nil {
		return nil, errors.Wrap(err)
	}

	q.outQ = outQ

	q.errC = make(chan error)

	var wg sync.WaitGroup

	output := func(reader Queue) {
		defer wg.Done()

		for {
			v, closed, err := reader.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				q.errC <- errors.Wrap(err)
			}

			err = outQ.Enqueue(v)
			if err != nil {
				q.errC <- errors.Wrap(err)
			}
		}
	}

	wg.Add(len(q.inQs))

	for _, inQ := range q.inQs {
		go output(inQ)
	}

	go func() {
		wg.Wait()

		err := q.outQ.Close()
		if err != nil {
			q.errC <- errors.Wrap(err)
		}
	}()

	return q, nil
}

func (q *joinedQueue) Enqueue(v interface{}) error {
	return q.outQ.Enqueue(v)
}

func (q *joinedQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
	select {
	case err := <-q.errC:
		return nil, false, errors.Wrap(err)
	default:
		return q.outQ.Dequeue(peek...)
	}
}

func (q *joinedQueue) Close() error {
	return q.outQ.Close()
}

func Join(inQs ...Queue) (Queue, error) {
	return newJoinedQueue(inQs...)
}

func Split(inQ Queue, outs ...func(outQ Queue, v interface{}) error) ([]Queue, Queue, error) {
	outQs := []Queue{}

	for range outs {
		outQ, err := NewChanQueue()
		if err != nil {
			return nil, nil, errors.Wrap(err)
		}

		outQs = append(outQs, outQ)
	}

	errQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	go func() {
		defer func() {
			for _, outQ := range outQs {
				err := outQ.Close()
				if err != nil {
					errQ.Enqueue(errors.Wrap(err))
				}
			}
		}()

		defer errQ.Close()

		for {
			v, closed, err := inQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				errQ.Enqueue(errors.Wrap(err))
			}

			for i, outQ := range outQs {
				err := outs[i](outQ, v)
				if err != nil {
					errQ.Enqueue(errors.Wrap(err))
				}
			}
		}
	}()

	return outQs, errQ, nil
}

func Map(inQ Queue, f func(v interface{}) (interface{}, error)) (Queue, Queue, error) {
	outQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	return MapTo(outQ, inQ, f)
}

func MapTo(outQ, inQ Queue, f func(v interface{}) (interface{}, error)) (Queue, Queue, error) {
	errQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	go func() {
		defer outQ.Close()

		defer errQ.Close()

		for {
			v, closed, err := inQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				errQ.Enqueue(errors.Wrap(err))
			}

			v, err = f(v)
			if err != nil {
				errQ.Enqueue(errors.Wrap(err))
			}

			err = outQ.Enqueue(v)
			if err != nil {
				errQ.Enqueue(errors.Wrap(err))
			}
		}
	}()

	return outQ, errQ, nil
}

func Reduce(inQ Queue, f func(accV, v interface{}) (interface{}, error)) (Queue, Queue, error) {
	outQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	return ReduceTo(outQ, inQ, f)
}

func ReduceTo(outQ, inQ Queue, f func(accV, v interface{}) (interface{}, error)) (Queue, Queue, error) {
	errQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	go func() {
		defer outQ.Close()

		defer errQ.Close()

		var accV interface{}

		for {
			v, closed, err := inQ.Dequeue()
			switch {
			case closed:
				err = outQ.Enqueue(accV)
				if err != nil {
					errQ.Enqueue(errors.Wrap(err))
				}

				return
			case err != nil:
				errQ.Enqueue(errors.Wrap(err))
			}

			accV, err = f(accV, v)
			if err != nil {
				errQ.Enqueue(errors.Wrap(err))
			}
		}
	}()

	return outQ, errQ, nil
}

func Filter(inQ Queue, f func(v interface{}) (bool, error)) (Queue, Queue, error) {
	outQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	return FilterTo(outQ, inQ, f)
}

func FilterTo(outQ, inQ Queue, f func(v interface{}) (bool, error)) (Queue, Queue, error) {
	errQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	go func() {
		defer outQ.Close()

		defer errQ.Close()

		for {
			v, closed, err := inQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				errQ.Enqueue(errors.Wrap(err))
			}

			enqueue, err := f(v)
			if err != nil {
				errQ.Enqueue(errors.Wrap(err))
			}

			if enqueue {
				err = outQ.Enqueue(v)
				if err != nil {
					errQ.Enqueue(errors.Wrap(err))
				}
			}
		}
	}()

	return outQ, errQ, nil
}

func PartitionBy(inQ Queue, f func(partitionV, v interface{}) (interface{}, error)) (Queue, Queue, error) {
	outQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	return PartitionByTo(outQ, inQ, f)
}

func PartitionByTo(outQ, inQ Queue, f func(partitionV, v interface{}) (interface{}, error)) (Queue, Queue, error) {
	errQ, err := NewChanQueue()
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	go func() {
		defer outQ.Close()

		defer errQ.Close()

		var partitionV interface{}

		for {
			v, closed, err := inQ.Dequeue()
			switch {
			case closed:
				err = outQ.Enqueue(partitionV)
				if err != nil {
					errQ.Enqueue(errors.Wrap(err))
				}

				return
			case err != nil:
				errQ.Enqueue(errors.Wrap(err))
			}

			newPartitionV, err := f(partitionV, v)
			if err != nil {
				errQ.Enqueue(errors.Wrap(err))
			}

			switch {
			case partitionV == nil:
				partitionV = newPartitionV
			case partitionV != nil && partitionV != newPartitionV:
				err = outQ.Enqueue(partitionV)
				if err != nil {
					errQ.Enqueue(errors.Wrap(err))
				}

				partitionV = newPartitionV
			}
		}
	}()

	return outQ, errQ, nil
}

func Compose(inQ Queue, fs ...interface{}) (Queue, Queue, error) {
	var outQ Queue

	errQs := make([]Queue, len(fs)/2)

	for i := 0; i < len(fs); i += 2 {
		paramVs := []reflect.Value{
			reflect.ValueOf(inQ),
			reflect.ValueOf(fs[i+1]),
		}

		returnVs := reflect.ValueOf(fs[i]).Call(paramVs)

		outQ = returnVs[0].Interface().(Queue)

		errQs[i/2] = returnVs[1].Interface().(Queue)

		err := returnVs[2].Interface()
		if err != nil {
			return nil, nil, errors.Wrap(err)
		}

		inQ = outQ
	}

	errQ, err := Join(errQs...)
	if err != nil {
		return nil, nil, errors.Wrap(err)
	}

	return outQ, errQ, nil
}
