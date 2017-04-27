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
	Open() error
	Close() error
	Enqueue(v interface{}) error
	Dequeue(peek ...bool) (interface{}, bool, error)
}

type ChanQueue struct {
	Capacity int
	elements chan interface{}
}

func (q *ChanQueue) Open() error {
	if q.Capacity < 0 {
		return errors.New("queues: capacity must be greater or equal than zero")
	}

	q.elements = make(chan interface{}, q.Capacity)

	return nil
}

func (q *ChanQueue) Close() error {
	close(q.elements)

	return nil
}

func (q *ChanQueue) Enqueue(v interface{}) error {
	q.elements <- v

	return nil
}

func (q *ChanQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
	if len(peek) != 0 {
		return nil, false, errors.New("queues: peek not supported")
	}

	v, open := <-q.elements

	return v, !open, nil
}

type ListQueue struct {
	elements *list.List
	lock     *sync.Mutex
	cond     *sync.Cond
	closed   bool
}

func (q *ListQueue) Open() error {
	q.elements = list.New()

	q.lock = &sync.Mutex{}

	q.cond = sync.NewCond(q.lock)

	return nil
}

func (q *ListQueue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.closed = true

	q.cond.Signal()

	return nil
}

func (q *ListQueue) enqueue(v interface{}) {
	q.elements.PushBack(v)
}

func (q *ListQueue) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.enqueue(v)

	q.cond.Signal()

	return nil
}

func (q *ListQueue) length() int {
	return q.elements.Len()
}

func (q *ListQueue) dequeue(peek bool) interface{} {
	listElement := q.elements.Front()

	element := listElement.Value

	if !peek {
		q.elements.Remove(listElement)
	}

	return element
}

func (q *ListQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
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

type BoltDbQueue struct {
	Db           *bolt.DB
	ElementTypes []interface{}
	elementTypes map[string]reflect.Type
	lock         *sync.Mutex
	cond         *sync.Cond
	closed       bool
}

func (q *BoltDbQueue) Open() error {
	if q.Db == nil {
		return errors.New("queues: db must be non-null")
	}

	if len(q.ElementTypes) == 0 {
		return errors.New("queues: at least one element type is required")
	}

	err := q.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("queue"))
		if err != nil {
			return errors.Wrap(err)
		}

		return nil
	})
	if err != nil {
		return errors.Wrap(err)
	}

	q.elementTypes = map[string]reflect.Type{}

	for _, v := range q.ElementTypes {
		q.elementTypes[reflect.ValueOf(v).Type().Name()] = reflect.ValueOf(v).Type()
	}

	q.lock = &sync.Mutex{}

	q.cond = sync.NewCond(q.lock)

	return nil
}

func (q *BoltDbQueue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.closed = true

	q.cond.Signal()

	return nil
}

func (q *BoltDbQueue) enqueue(v interface{}) error {
	err := q.Db.Update(func(tx *bolt.Tx) error {
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

func (q *BoltDbQueue) Enqueue(v interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	err := q.enqueue(v)
	if err != nil {
		return errors.Wrap(err)
	}

	q.cond.Signal()

	return nil
}

func (q *BoltDbQueue) length() (int, error) {
	var length int

	err := q.Db.View(func(tx *bolt.Tx) error {
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

func (q *BoltDbQueue) dequeue(peek bool) (interface{}, error) {
	var v interface{}

	err := q.Db.Update(func(tx *bolt.Tx) error {
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
			return errors.New("queues: element type is invalid")
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

func (q *BoltDbQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
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

type ComposeFunc func(q Queue) (Queue, error)

func Compose(fs ...ComposeFunc) ComposeFunc {
	return ComposeFunc(func(inQ Queue) (Queue, error) {
		for _, f := range fs {
			var err error

			inQ, err = f(inQ)
			if err != nil {
				return nil, errors.Wrap(err)
			}
		}

		return inQ, nil
	})
}

type MapFunc func(v interface{}) (interface{}, error)

func Map(f MapFunc) ComposeFunc {
	return MapTo(&ChanQueue{}, f)
}

func MapTo(outQ Queue, f MapFunc) ComposeFunc {
	return ComposeFunc(func(inQ Queue) (Queue, error) {
		if inQ == nil {
			return nil, errors.New("queues: in queue must be non-null")
		}

		if outQ == nil {
			return nil, errors.New("queues: out queue must be non-null")
		}

		if f == nil {
			return nil, errors.New("queues: map function must be non-null")
		}

		err := outQ.Open()
		if err != nil {
			return nil, errors.Wrap(err)
		}

		go func() {
			defer func() {
				cerr := outQ.Close()
				if cerr != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}()

			for {
				v, closed, err := inQ.Dequeue()
				switch {
				case closed:
					return
				case err != nil:
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				v, err = f(v)
				if err != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}

					continue
				}

				err = outQ.Enqueue(v)
				if err != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}
		}()

		return outQ, nil
	})
}

type ReduceFunc func(acc, v interface{}) (interface{}, error)

func Reduce(f ReduceFunc) ComposeFunc {
	return ReduceTo(&ChanQueue{}, f)
}

func ReduceTo(outQ Queue, f ReduceFunc) ComposeFunc {
	return ComposeFunc(func(inQ Queue) (Queue, error) {
		if inQ == nil {
			return nil, errors.New("queues: in queue must be non-null")
		}

		if outQ == nil {
			return nil, errors.New("queues: out queue must be non-null")
		}

		if f == nil {
			return nil, errors.New("queues: reduce function must be non-null")
		}

		err := outQ.Open()
		if err != nil {
			return nil, errors.Wrap(err)
		}

		go func() {
			defer func() {
				cerr := outQ.Close()
				if cerr != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}()

			var acc interface{}

			for {
				v, closed, err := inQ.Dequeue()
				switch {
				case closed:
					err = outQ.Enqueue(acc)
					if err != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}

					return
				case err != nil:
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				acc, err = f(acc, v)
				if err != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}
		}()

		return outQ, nil
	})
}

type FilterFunc func(v interface{}) (bool, error)

func Filter(f FilterFunc) ComposeFunc {
	return FilterTo(&ChanQueue{}, f)
}

func FilterTo(outQ Queue, f FilterFunc) ComposeFunc {
	return ComposeFunc(func(inQ Queue) (Queue, error) {
		if inQ == nil {
			return nil, errors.New("queues: in queue must be non-null")
		}

		if outQ == nil {
			return nil, errors.New("queues: out queue must be non-null")
		}

		if f == nil {
			return nil, errors.New("queues: filter function must be non-null")
		}

		err := outQ.Open()
		if err != nil {
			return nil, errors.Wrap(err)
		}

		go func() {
			defer func() {
				cerr := outQ.Close()
				if cerr != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}()

			for {
				v, closed, err := inQ.Dequeue()
				switch {
				case closed:
					return
				case err != nil:
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				enqueue, err := f(v)
				if err != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				if enqueue {
					err = outQ.Enqueue(v)
					if err != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}
				}
			}
		}()

		return outQ, nil
	})
}

type PartitionFunc func(partition, v interface{}) (interface{}, error)

func Partition(f PartitionFunc) ComposeFunc {
	return PartitionTo(&ChanQueue{}, f)
}

func PartitionTo(outQ Queue, f PartitionFunc) ComposeFunc {
	return ComposeFunc(func(inQ Queue) (Queue, error) {
		if inQ == nil {
			return nil, errors.New("queues: in queue must be non-null")
		}

		if outQ == nil {
			return nil, errors.New("queues: out queue must be non-null")
		}

		if f == nil {
			return nil, errors.New("queues: partition function must be non-null")
		}

		err := outQ.Open()
		if err != nil {
			return nil, errors.Wrap(err)
		}

		go func() {
			defer func() {
				cerr := outQ.Close()
				if cerr != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}()

			var partition interface{}

			for {
				v, closed, err := inQ.Dequeue()
				switch {
				case closed:
					err = outQ.Enqueue(partition)
					if err != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}

					return
				case err != nil:
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				newPartition, err := f(partition, v)
				if err != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				if partition != nil && partition != newPartition {
					err = outQ.Enqueue(partition)
					if err != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}
				}

				partition = newPartition
			}
		}()

		return outQ, nil
	})
}

type ForkJoinFunc func(v interface{}) (map[int]interface{}, error)

func ForkJoin(f ForkJoinFunc, cfs ...ComposeFunc) ComposeFunc {
	return ForkJoinTo(&ChanQueue{}, f, cfs...)
}

func ForkJoinTo(outQ Queue, f ForkJoinFunc, cfs ...ComposeFunc) ComposeFunc {
	return ComposeFunc(func(inQ Queue) (Queue, error) {
		if inQ == nil {
			return nil, errors.New("queues: in queue must be non-null")
		}

		if outQ == nil {
			return nil, errors.New("queues: out queue must be non-null")
		}

		if f == nil {
			return nil, errors.New("queues: fork join function must be non-null")
		}

		if len(cfs) == 0 {
			return nil, errors.New("queues: at least one compose function is required")
		}

		cfInQs := []Queue{}

		cfOutQs := []Queue{}

		for _, cf := range cfs {
			cfInQ := &ChanQueue{}

			err := cfInQ.Open()
			if err != nil {
				return nil, errors.Wrap(err)
			}

			cfInQs = append(cfInQs, cfInQ)

			cfOutQ, err := cf(cfInQ)
			if err != nil {
				return nil, errors.Wrap(err)
			}

			cfOutQs = append(cfOutQs, cfOutQ)
		}

		err := outQ.Open()
		if err != nil {
			return nil, errors.Wrap(err)
		}

		go func() {
			defer func() {
				for _, cfInQ := range cfInQs {
					cerr := cfInQ.Close()
					if cerr != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}
				}
			}()

			for {
				v, closed, err := inQ.Dequeue()
				switch {
				case closed:
					return
				case err != nil:
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				cfMapping, err := f(v)
				if err != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}

				for i, v := range cfMapping {
					if len(cfs) <= i {
						eerr := outQ.Enqueue(errors.New("queues: compose function mapping is invalid"))
						if eerr != nil {
							errors.Print(err)
						}
					}

					err = cfInQs[i].Enqueue(v)
					if err != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}
				}
			}
		}()

		var wg sync.WaitGroup

		wg.Add(len(cfs))

		for _, cfOutQ := range cfOutQs {
			go func(cfOutQ Queue) {
				defer wg.Done()

				for {
					v, closed, err := cfOutQ.Dequeue()
					switch {
					case closed:
						return
					case err != nil:
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}

					err = outQ.Enqueue(v)
					if err != nil {
						eerr := outQ.Enqueue(errors.Wrap(err))
						if eerr != nil {
							errors.Print(err)
						}
					}
				}
			}(cfOutQ)
		}

		go func() {
			defer func() {
				cerr := outQ.Close()
				if cerr != nil {
					eerr := outQ.Enqueue(errors.Wrap(err))
					if eerr != nil {
						errors.Print(err)
					}
				}
			}()

			wg.Wait()
		}()

		return outQ, nil
	})
}
