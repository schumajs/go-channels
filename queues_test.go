/*
  Copyright 2016 Jens Schumann <schumajs@gmail.com>

  Use of this source code is governed by the MIT license that can be found in
  the LICENSE file.
*/

package queues

import (
	"io/ioutil"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
)

type newQueueFunc func() (Queue, error)

var testInputs = map[string]newQueueFunc{
	"ListQueue": newQueueFunc(func() (Queue, error) {
		return NewListQueue()
	}),
	"BoltDbQueue": newQueueFunc(func() (Queue, error) {
		tempFile, err := ioutil.TempFile("", "")
		if err != nil {
			return nil, err
		}

		tempFilePath := tempFile.Name()

		err = tempFile.Close()
		if err != nil {
			return nil, err
		}

		db, err := bolt.Open(tempFilePath, 0600, nil)
		if err != nil {
			return nil, err
		}

		return NewBoltDbQueue(db, []interface{}{int(0)})
	}),
}

func nonNegativeInts(t *testing.T, newQueue newQueueFunc, count int) Queue {
	outQ, err := newQueue()
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		defer outQ.Close()

		for i := 0; i < count; i++ {
			err = outQ.Enqueue(i)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	return outQ
}

func square(t *testing.T, newQueue newQueueFunc, inQ Queue) Queue {
	outQ, err := newQueue()
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		defer outQ.Close()

		for {
			v, closed, err := inQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				t.Fatal(err)
			}

			outQ.Enqueue(v.(int) * v.(int))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	return outQ
}

func testQueue(t *testing.T, newQueue newQueueFunc) {
	nonNegativeIntsQ := nonNegativeInts(t, newQueue, 1000)

	for i := 0; ; i++ {
		v, closed, err := nonNegativeIntsQ.Dequeue(true)
		switch {
		case closed:
			return
		case err != nil:
			t.Fatal(err)
		}

		if v.(int) != i {
			t.Fatalf("expected v<%d> = %d", v, i)
		}

		_, closed, err = nonNegativeIntsQ.Dequeue()
		switch {
		case closed:
			t.Fatalf("expected queue to be open")
		case err != nil:
			t.Fatal(err)
		}
	}
}

func TestQueue(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testQueue(t, newQueue)
		})
	}
}

func testJoin(t *testing.T, newQueue newQueueFunc) {
	nonNegativeIntsQ := nonNegativeInts(t, newQueue, 1000)

	squaresQ1 := square(t, newQueue, nonNegativeIntsQ)

	squaresQ2 := square(t, newQueue, nonNegativeIntsQ)

	squaresQ, errQ, err := Join(squaresQ1, squaresQ2)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			_, closed, err := errQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				t.Fatal(err)
			}
		}
	}()

	sum := 0

	for i := 0; ; i++ {
		v, closed, err := squaresQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		sum = sum + v.(int)
	}

	if sum != 332833500 {
		t.Fatalf("expected sum<%d> = 332833500", sum)
	}
}

func TestJoin(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testJoin(t, newQueue)
		})
	}
}

func testSplit(t *testing.T, newQueue newQueueFunc) {
	nonNegativeIntsQ := nonNegativeInts(t, newQueue, 1000)

	oddAndEvenQs, errQ, err := Split(
		nonNegativeIntsQ,
		func(outQ Queue, v interface{}) error {
			if v.(int)%2 != 0 {
				err := outQ.Enqueue(v)
				if err != nil {
					return err
				}
			}

			return nil
		},
		func(outQ Queue, v interface{}) error {
			if v.(int)%2 == 0 {
				err := outQ.Enqueue(v)
				if err != nil {
					return err
				}
			}

			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			_, closed, err := errQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				t.Fatal(err)
			}
		}
	}()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 1; ; i += 2 {
			v, closed, err := oddAndEvenQs[0].Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				t.Fatal(err)
			}

			if v.(int) != i {
				t.Fatalf("expected v<%d> = %d", v, i)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; ; i += 2 {
			v, closed, err := oddAndEvenQs[1].Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				t.Fatal(err)
			}

			if v.(int) != i {
				t.Fatalf("expected v<%d> = %d", v, i)
			}
		}
	}()

	wg.Wait()
}

func TestSplit(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testSplit(t, newQueue)
		})
	}
}

type chanQueue struct {
	elements chan interface{}
}

func newChanQueue() (Queue, error) {
	q := &chanQueue{}

	q.elements = make(chan interface{})

	return q, nil
}

func (q *chanQueue) Dequeue(peek ...bool) (interface{}, bool, error) {
	// peek not supported

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

func benchmarkQueue(b *testing.B, queue newQueueFunc) {
	nonNegativeInts := func(count int) Queue {
		outQ, err := queue()
		if err != nil {
			b.Fatal(err)
		}

		go func() {
			defer outQ.Close()

			for i := 0; i < count; i++ {
				err = outQ.Enqueue(i)
				if err != nil {
					b.Fatal(err)
				}
			}
		}()

		return outQ
	}

	nonNegativeIntsQ := nonNegativeInts(100000)

	for i := 0; ; i++ {
		v, closed, err := nonNegativeIntsQ.Dequeue()
		switch {
		case closed:
			return
		case err != nil:
			b.Fatal(err)
		}

		if v.(int) != i {
			b.Fatalf("expected v<%d> = %d", v, i)
		}
	}
}

func BenchmarkQueue(b *testing.B) {
	testInputs["ChanQueue"] = newQueueFunc(func() (Queue, error) {
		return newChanQueue()
	})
	defer delete(testInputs, "chanQueue")

	for name, newQueue := range testInputs {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkQueue(b, newQueue)
			}
		})
	}
}
