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

type Dequeuer interface {
	Dequeue(peek ...bool) (interface{}, bool, error)
}

type Enqueuer interface {
	Enqueue(v interface{}) error
	Close() error
}

type Queuer interface {
	Dequeuer
	Enqueuer
}

type listQueuer struct {
	elements *list.List
	lock     *sync.Mutex
	cond     *sync.Cond
	closed   bool
}

func NewListQueuer() (Queuer, error) {
	q := &listQueuer{}

	q.elements = list.New()

	q.lock = &sync.Mutex{}

	q.cond = sync.NewCond(q.lock)

	return q, nil
}

func (q *listQueuer) length() int {
	return q.elements.Len()
}

func (q *listQueuer) dequeue(peek bool) interface{} {
	listElement := q.elements.Front()

	element := listElement.Value

	if !peek {
		q.elements.Remove(listElement)
	}

	return element
}

func (q *listQueuer) enqueue(v interface{}) {
	q.elements.PushBack(v)
}

func (q *listQueuer) Dequeue(peek ...bool) (interface{}, bool, error) {
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

func (q *listQueuer) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.enqueue(v)

	q.cond.Signal()

	return nil
}

func (q *listQueuer) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.closed = true

	q.cond.Signal()

	return nil
}

type boltDbQueuer struct {
	db           *bolt.DB
	elementTypes map[string]reflect.Type
	lock         *sync.Mutex
	cond         *sync.Cond
	closed       bool
}

func NewBoltDbQueuer(db *bolt.DB, elementTypes []interface{}) (Queuer, error) {
	q := &boltDbQueuer{}

	q.db = db

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("boltDbQueuer"))
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

func (q *boltDbQueuer) length() (int, error) {
	var length int

	err := q.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("boltDbQueuer"))

		stats := bucket.Stats()

		length = stats.KeyN

		return nil
	})
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (q *boltDbQueuer) dequeue(peek bool) (interface{}, error) {
	var v interface{}

	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("boltDbQueuer"))

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

func (q *boltDbQueuer) enqueue(v interface{}) error {
	err := q.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("boltDbQueuer"))

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

func (q *boltDbQueuer) Dequeue(peek ...bool) (interface{}, bool, error) {
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

func (q *boltDbQueuer) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	err := q.enqueue(v)
	if err != nil {
		return err
	}

	q.cond.Signal()

	return nil
}

func (q *boltDbQueuer) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.closed = true

	q.cond.Signal()

	return nil
}

type multiplexedDequeuer struct {
	inDeqs []Dequeuer
	outQ   Queuer
	errC   chan error
}

func NewMultiplexedDequeuer(inDeqs ...Dequeuer) (Dequeuer, error) {
	mr := &multiplexedDequeuer{}

	mr.inDeqs = inDeqs

	outQ, err := NewListQueuer()
	if err != nil {
		return nil, err
	}

	mr.outQ = outQ

	mr.errC = make(chan error)

	var wg sync.WaitGroup

	output := func(reader Dequeuer) {
		defer wg.Done()

		for {
			v, closed, err := reader.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				mr.errC <- err
			}

			outQ.Enqueue(v)
		}
	}

	wg.Add(len(mr.inDeqs))

	for _, inDeq := range mr.inDeqs {
		go output(inDeq)
	}

	go func() {
		wg.Wait()

		err := mr.outQ.Close()
		if err != nil {
			mr.errC <- err
		}
	}()

	return mr, nil
}

func (mr *multiplexedDequeuer) Dequeue(peek ...bool) (interface{}, bool, error) {
	select {
	case err := <-mr.errC:
		return nil, false, err
	default:
		return mr.outQ.Dequeue(peek...)
	}
}
