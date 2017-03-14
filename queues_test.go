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

func nonNegInts(newQueue newQueueFunc, count int) (Queue, Queue, error) {
	outQ, err := newQueue()
	if err != nil {
		return nil, nil, err
	}

	errQ, err := NewListQueue()
	if err != nil {
		return nil, nil, err
	}

	go func() {
		defer outQ.Close()

		defer errQ.Close()

		for i := 0; i < count; i++ {
			err = outQ.Enqueue(i)
			if err != nil {
				errQ.Enqueue(err)
			}
		}
	}()

	return outQ, errQ, nil
}

func testQueue(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, errQ, err := nonNegInts(newQueue, 1000)
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

	for i := 0; ; i++ {
		v, closed, err := nonNegIntsQ.Dequeue(true)
		switch {
		case closed:
			return
		case err != nil:
			t.Fatal(err)
		}

		if v.(int) != i {
			t.Fatalf("expected v<%d> = %d", v, i)
		}

		_, closed, err = nonNegIntsQ.Dequeue()
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
	nonNegIntsQ1, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	nonNegIntsQ2, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	joinedQ, err := Join(nonNegIntsQ1, nonNegIntsQ2)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			_, closed, err := nonNegIntsErrQ.Dequeue()
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
		v, closed, err := joinedQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		sum = sum + v.(int)
	}

	if sum != 999000 {
		t.Fatalf("expected sum<%d> = 332833500", sum)
	}
}

func TestJoin(t *testing.T) {
	testJoin(t, testInputs["ListQueue"])
}

func testSplit(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	oddsAndEvensQs, oddsAndEvensErrQ, err := Split(
		nonNegIntsQ,
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

	errQ, err := Join(nonNegIntsErrQ, oddsAndEvensErrQ)
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
			v, closed, err := oddsAndEvensQs[0].Dequeue()
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
			v, closed, err := oddsAndEvensQs[1].Dequeue()
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
	testSplit(t, testInputs["ListQueue"])
}

func testMap(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	squaresQ, squaresErrQ, err := Map(nonNegIntsQ, func(v interface{}) (interface{}, error) {
		return v.(int) * v.(int), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	errQ, err := Join(nonNegIntsErrQ, squaresErrQ)
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

	for i := 0; ; i++ {
		v, closed, err := squaresQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		if v.(int) != i*i {
			t.Fatalf("expected v<%d> = %d", v, i*i)
		}
	}
}

func TestMap(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testMap(t, newQueue)
		})
	}
}

func testReduce(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	sumQ, sumErrQ, err := Reduce(nonNegIntsQ, func(accV, v interface{}) (interface{}, error) {
		switch {
		case accV == nil:
			return int(0), nil
		default:
			return accV.(int) + v.(int), nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	errQ, err := Join(nonNegIntsErrQ, sumErrQ)
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

	v, _, err := sumQ.Dequeue()
	if err != nil {
		t.Fatal(err)
	}

	sum := v.(int)

	if sum != 499500 {
		t.Fatalf("expected sum<%d> = 499500", sum)
	}
}

func TestReduce(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testReduce(t, newQueue)
		})
	}
}

func testFilter(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	oddsQ, oddsErrQ, err := Filter(nonNegIntsQ, func(v interface{}) (bool, error) {
		return v.(int)%2 == 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	errQ, err := Join(nonNegIntsErrQ, oddsErrQ)
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

	for i := 1; ; i += 2 {
		v, closed, err := oddsQ.Dequeue()
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
}

func TestFilter(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testFilter(t, newQueue)
		})
	}
}

func testPartitionBy(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	partitionsQ, partitionsErrQ, err := PartitionBy(nonNegIntsQ, func(partitionV, v interface{}) (interface{}, error) {
		switch {
		case partitionV == nil || v.(int)%10 == 0:
			return &[]int{v.(int)}, nil
		default:
			partition := partitionV.(*[]int)

			*partition = append(*partition, v.(int))

			return partition, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	errQ, err := Join(nonNegIntsErrQ, partitionsErrQ)
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

	partitionsCount := 0

	for {
		v, closed, err := partitionsQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		partitionsCount++

		if len(*v.(*[]int)) != 10 {
			t.Fatalf("expected v<%d> = %d", len(*v.(*[]int)), 10)
		}
	}

	if partitionsCount != 100 {
		t.Fatalf("expected partitions count<%d> = 100", partitionsCount)
	}
}

func TestPartitionBy(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testPartitionBy(t, newQueue)
		})
	}
}

func testCompose(t *testing.T, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 1000)
	if err != nil {
		t.Fatal(err)
	}

	square := func(v interface{}) (interface{}, error) {
		return v.(int) * v.(int), nil
	}

	sum := func(accV, v interface{}) (interface{}, error) {
		switch {
		case accV == nil:
			return int(0), nil
		default:
			return accV.(int) + v.(int), nil
		}
	}

	sumOfSquaresQ, sumOfSquaresErrQ, err := Compose(nonNegIntsQ, Map, square, Reduce, sum)
	if err != nil {
		t.Fatal(err)
	}

	errQ, err := Join(nonNegIntsErrQ, sumOfSquaresErrQ)
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

	v, _, err := sumOfSquaresQ.Dequeue()
	if err != nil {
		t.Fatal(err)
	}

	if v.(int) != 332833500 {
		t.Fatalf("expected v<%d> = 332833500", v.(int))
	}
}

func TestCompose(t *testing.T) {
	for name, newQueue := range testInputs {
		t.Run(name, func(t *testing.T) {
			testCompose(t, newQueue)
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

func benchmarkQueue(b *testing.B, newQueue newQueueFunc) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(newQueue, 100000)
	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for {
			_, closed, err := nonNegIntsErrQ.Dequeue()
			switch {
			case closed:
				return
			case err != nil:
				b.Fatal(err)
			}
		}
	}()

	for i := 0; ; i++ {
		v, closed, err := nonNegIntsQ.Dequeue()
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
