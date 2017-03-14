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
	"errors"
	"reflect"
	"sync"

	"github.com/boltdb/bolt"
)

type Queue interface {
	Enqueue(v interface{}) error
	Dequeue(peek ...bool) (interface{}, bool, error)
	Close() error
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
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
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
		return 0, err
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
			return err
		}

		reflectType, ok := q.elementTypes[typeName]
		if !ok {
			return errors.New("invalid element type")
		}

		reflectValue := reflect.Indirect(reflect.New(reflectType))

		err = decoder.DecodeValue(reflectValue)
		if err != nil {
			return err
		}

		v = reflectValue.Interface()

		if !peek {
			err = cursor.Delete()
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
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
			return err
		}

		err = encoder.Encode(v)
		if err != nil {
			return err
		}

		err = bucket.Put(key, buffer.Bytes())
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
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
		return nil, false, err
	}

	for !(length > 0 || q.closed) {
		q.cond.Wait()

		length, err = q.length()
		if err != nil {
			return nil, false, err
		}
	}

	switch {
	case length > 0:
		v, err := q.dequeue(peek[0])
		if err != nil {
			return nil, false, err
		}

		return v, false, err
	case q.closed:
		return nil, true, err
	}

	return nil, false, nil
}

func (q *boltDbQueue) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	err := q.enqueue(v)
	if err != nil {
		return err
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

	outQ, err := NewListQueue()
	if err != nil {
		return nil, err
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
				q.errC <- err
			}

			err = outQ.Enqueue(v)
			if err != nil {
				q.errC <- err
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
			q.errC <- err
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
		return nil, false, err
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
		outQ, err := NewListQueue()
		if err != nil {
			return nil, nil, err
		}

		outQs = append(outQs, outQ)
	}

	errQ, err := NewListQueue()
	if err != nil {
		return nil, nil, err
	}

	go func() {
		defer func() {
			for _, outQ := range outQs {
				err := outQ.Close()
				if err != nil {
					errQ.Enqueue(err)
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
				errQ.Enqueue(err)
			}

			for i, outQ := range outQs {
				err := outs[i](outQ, v)
				if err != nil {
					errQ.Enqueue(err)
				}
			}
		}
	}()

	return outQs, errQ, nil
}

func Pipe(inQ Queue, fs ...func(v interface{}) (interface{}, bool, error)) (Queue, Queue, error) {
	var outQ Queue

	errQs := make([]Queue, len(fs))

	for i, f := range fs {
		var err error

		outQ, err = NewListQueue()
		if err != nil {
			return nil, nil, err
		}

		errQs[i], err = NewListQueue()
		if err != nil {
			return nil, nil, err
		}

		go func(inQ, outQ, errQ Queue, f func(v interface{}) (interface{}, bool, error)) {
			defer outQ.Close()

			defer errQ.Close()

			for {
				v, closed, err := inQ.Dequeue()
				switch {
				case closed:
					return
				case err != nil:
					errQ.Enqueue(err)
				}

				var enqueue bool

				v, enqueue, err = f(v)
				if err != nil {
					errQ.Enqueue(err)
				}

				if enqueue {
					err = outQ.Enqueue(v)
					if err != nil {
						errQ.Enqueue(err)
					}
				}
			}
		}(inQ, outQ, errQs[i], f)

		inQ = outQ
	}

	errQ, err := Join(errQs...)
	if err != nil {
		return nil, nil, err
	}

	return outQ, errQ, nil
}
