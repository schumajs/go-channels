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
	"ChanQueue": newQueueFunc(func() (Queue, error) {
		return NewChanQueue()
	}),
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

func nonNegInts(testInput string, count int) (Queue, Queue, error) {
	outQ, err := testInputs[testInput]()
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

func testQueue(t *testing.T, testInput string) {
	nonNegIntsQ, errQ, err := nonNegInts(testInput, 1000)
	if err != nil {
		t.Fatal(err)
	}

	errC := make(chan error)

	go func() {
		defer close(errC)

		v, closed, derr := errQ.Dequeue()
		switch {
		case closed:
			return
		case derr != nil:
			t.Fatal(derr)
		case v != nil:
			errC <- v.(error)
		}
	}()

	for i := 0; ; i++ {
		if testInput == "ChanQueue" {
			v, closed, err := nonNegIntsQ.Dequeue()
			if closed {
				break
			}
			if err != nil {
				t.Fatal(err)
			}

			if v.(int) != i {
				t.Fatalf("expected v<%d> = %d", v, i)
			}
		} else {
			v, closed, err := nonNegIntsQ.Dequeue(true)
			if closed {
				break
			}
			if err != nil {
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

	err = <-errC
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueue(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testQueue(t, testInput)
		})
	}
}

func testJoin(t *testing.T, testInput string) {
	nonNegIntsQ1, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
	if err != nil {
		t.Fatal(err)
	}

	nonNegIntsQ2, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
	if err != nil {
		t.Fatal(err)
	}

	joinedQ, err := Join(nonNegIntsQ1, nonNegIntsQ2)
	if err != nil {
		t.Fatal(err)
	}

	errC := make(chan error)

	go func() {
		defer close(errC)

		v, closed, derr := nonNegIntsErrQ.Dequeue()
		switch {
		case closed:
			return
		case derr != nil:
			t.Fatal(derr)
		case v != nil:
			errC <- v.(error)
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

	err = <-errC
	if err != nil {
		t.Fatal(err)
	}
}

func TestJoin(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testJoin(t, testInput)
		})
	}
}

func testSplit(t *testing.T, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
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

	errC := make(chan error)

	go func() {
		defer close(errC)

		v, closed, derr := errQ.Dequeue()
		switch {
		case closed:
			return
		case derr != nil:
			t.Fatal(derr)
		case v != nil:
			errC <- v.(error)
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

	err = <-errC
	if err != nil {
		t.Fatal(err)
	}
}

func TestSplit(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testSplit(t, testInput)
		})
	}
}

func testMap(t *testing.T, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
	if err != nil {
		t.Fatal(err)
	}

	squaresQ, squaresErrQ, err := Map(nonNegIntsQ, func(v interface{}) (interface{}, error) {
		return v.(int) * v.(int), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	resultQ, err := Join(nonNegIntsErrQ, squaresQ, squaresErrQ)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; ; i++ {
		v, closed, err := resultQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case int:
			if v.(int) != i*i {
				t.Fatalf("expected v<%d> = %d", v, i*i)
			}
		case error:
			t.Fatal(v)
		}
	}
}

func TestMap(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testMap(t, testInput)
		})
	}
}

func testReduce(t *testing.T, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
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

	resultQ, err := Join(nonNegIntsErrQ, sumQ, sumErrQ)
	if err != nil {
		t.Fatal(err)
	}

	for {
		v, closed, err := resultQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case int:
			if v.(int) != 499500 {
				t.Fatalf("expected v<%d> = 499500", v.(int))
			}
		case error:
			t.Fatal(v)
		}
	}
}

func TestReduce(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testReduce(t, testInput)
		})
	}
}

func testFilter(t *testing.T, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
	if err != nil {
		t.Fatal(err)
	}

	oddsQ, oddsErrQ, err := Filter(nonNegIntsQ, func(v interface{}) (bool, error) {
		return v.(int)%2 == 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	resultQ, err := Join(nonNegIntsErrQ, oddsQ, oddsErrQ)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; ; i += 2 {
		v, closed, err := resultQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case int:
			if v.(int) != i {
				t.Fatalf("expected v<%d> = %d", v, i)
			}
		case error:
			t.Fatal(v)
		}
	}
}

func TestFilter(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testFilter(t, testInput)
		})
	}
}

func testPartitionBy(t *testing.T, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
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

	resultQ, err := Join(nonNegIntsErrQ, partitionsQ, partitionsErrQ)
	if err != nil {
		t.Fatal(err)
	}

	partitionsCount := 0

	for {
		v, closed, err := resultQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case *[]int:
			partitionsCount++

			if len(*v.(*[]int)) != 10 {
				t.Fatalf("expected v<%d> = %d", len(*v.(*[]int)), 10)
			}
		case error:
			t.Fatal(v)
		}
	}

	if partitionsCount != 100 {
		t.Fatalf("expected partitions count<%d> = 100", partitionsCount)
	}
}

func TestPartitionBy(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testPartitionBy(t, testInput)
		})
	}
}

func testCompose(t *testing.T, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 1000)
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

	resultQ, err := Join(nonNegIntsErrQ, sumOfSquaresQ, sumOfSquaresErrQ)
	if err != nil {
		t.Fatal(err)
	}

	for {
		v, closed, err := resultQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case int:
			if v.(int) != 332833500 {
				t.Fatalf("expected v<%d> = 332833500", v.(int))
			}
		case error:
			t.Fatal(v)
		}
	}
}

func TestCompose(t *testing.T) {
	for testInput := range testInputs {
		t.Run(testInput, func(t *testing.T) {
			testCompose(t, testInput)
		})
	}
}

func benchmarkQueue(b *testing.B, testInput string) {
	nonNegIntsQ, nonNegIntsErrQ, err := nonNegInts(testInput, 100000)
	if err != nil {
		b.Fatal(err)
	}

	errC := make(chan error)

	go func() {
		defer close(errC)

		v, closed, derr := nonNegIntsErrQ.Dequeue()
		switch {
		case closed:
			return
		case derr != nil:
			b.Fatal(derr)
		case v != nil:
			errC <- v.(error)
		}
	}()

	for i := 0; ; i++ {
		v, closed, err := nonNegIntsQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			b.Fatal(err)
		}

		if v.(int) != i {
			b.Fatalf("expected v<%d> = %d", v, i)
		}
	}

	err = <-errC
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkQueue(b *testing.B) {
	for testInput := range testInputs {
		b.Run(testInput, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkQueue(b, testInput)
			}
		})
	}
}
