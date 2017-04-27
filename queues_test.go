/*
  Copyright 2016 Jens Schumann <schumajs@gmail.com>

  Use of this source code is governed by the MIT license that can be found in
  the LICENSE file.
*/

package queues

import (
	"io/ioutil"
	"testing"

	"github.com/boltdb/bolt"
)

type newQueueFunc func(t testing.TB) Queue

type testee struct {
	Name     string
	NewQueue newQueueFunc
}

var testees = []*testee{
	{
		Name: "ChanQueue",
		NewQueue: newQueueFunc(func(t testing.TB) Queue {
			return &ChanQueue{}
		}),
	},
	{
		Name: "ListQueue",
		NewQueue: newQueueFunc(func(t testing.TB) Queue {
			return &ListQueue{}
		}),
	},
	{
		Name: "BoltDbQueue",
		NewQueue: newQueueFunc(func(t testing.TB) Queue {
			tempFile, err := ioutil.TempFile("", "")
			if err != nil {
				t.Fatal(err)
			}

			tempFilePath := tempFile.Name()

			err = tempFile.Close()
			if err != nil {
				t.Fatal(err)
			}

			db, err := bolt.Open(tempFilePath, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}

			return &BoltDbQueue{
				Db:           db,
				ElementTypes: []interface{}{int(0), &[]int{}},
			}
		}),
	},
}

func nonNegIntsQ(tb testing.TB, testee *testee, count int) Queue {
	q := testee.NewQueue(tb)

	err := q.Open()
	if err != nil {
		tb.Fatal(err)
	}

	go func() {
		defer func() {
			cerr := q.Close()
			if cerr != nil {
				tb.Fatal(cerr)
			}
		}()

		for i := 0; i < count; i++ {
			err = q.Enqueue(i)
			if err != nil {
				q.Enqueue(err)
			}
		}
	}()

	return q
}

func testQueue(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	for i := 0; ; i++ {
		if testee.Name == "ChanQueue" {
			v, closed, err := nonNegIntsQ.Dequeue()
			if closed {
				break
			}
			if err != nil {
				t.Fatal(err)
			}

			switch v.(type) {
			case error:
				t.Fatal(v)
			case int:
				if v.(int) != i {
					t.Errorf("v is %d, want %d", v, i)
				}
			}
		} else {
			v, closed, err := nonNegIntsQ.Dequeue(true)
			if closed {
				break
			}
			if err != nil {
				t.Fatal(err)
			}

			switch v.(type) {
			case error:
				t.Fatal(v)
			case int:
				if v.(int) != i {
					t.Errorf("v is %d, want %d", v, i)
				}
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
}

func TestQueue(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testQueue(t, testee)
		})
	}
}

func testCompose(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	square := MapFunc(func(v interface{}) (interface{}, error) {
		return v.(int) * v.(int), nil
	})

	sum := ReduceFunc(func(acc, v interface{}) (interface{}, error) {
		if acc == nil {
			acc = 0
		}

		return acc.(int) + v.(int), nil
	})

	sumOfSquaresQ, err := Compose(
		MapTo(testee.NewQueue(t), square),
		ReduceTo(testee.NewQueue(t), sum))(nonNegIntsQ)
	if err != nil {
		t.Fatal(err)
	}

	for {
		v, closed, err := sumOfSquaresQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case error:
			t.Fatal(v)
		case int:
			if v.(int) != 332833500 {
				t.Fatalf("v is %d, want 332833500", v.(int))
			}
		}
	}
}

func TestCompose(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testCompose(t, testee)
		})
	}
}

func testMap(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	squaresQ, err := MapTo(testee.NewQueue(t), MapFunc(func(v interface{}) (interface{}, error) {
		return v.(int) * v.(int), nil
	}))(nonNegIntsQ)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; ; i++ {
		v, closed, err := squaresQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case error:
			t.Fatal(v)
		case int:
			if v.(int) != i*i {
				t.Errorf("v is %d, want %d", v, i*i)
			}
		}
	}
}

func TestMap(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testMap(t, testee)
		})
	}
}

func testReduce(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	sumQ, err := ReduceTo(testee.NewQueue(t), ReduceFunc(func(acc, v interface{}) (interface{}, error) {
		if acc == nil {
			acc = 0
		}

		return acc.(int) + v.(int), nil
	}))(nonNegIntsQ)
	if err != nil {
		t.Fatal(err)
	}

	for {
		v, closed, err := sumQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case error:
			t.Fatal(v)
		case int:
			if v.(int) != 499500 {
				t.Fatalf("v is %d, want 499500", v.(int))
			}
		}
	}
}

func TestReduce(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testReduce(t, testee)
		})
	}
}

func testFilter(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	oddsQ, err := FilterTo(testee.NewQueue(t), FilterFunc(func(v interface{}) (bool, error) {
		return v.(int)%2 == 1, nil
	}))(nonNegIntsQ)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; ; i += 2 {
		v, closed, err := oddsQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case error:
			t.Fatal(v)
		case int:
			if v.(int) != i {
				t.Fatalf("v is %d, want %d", v, i)
			}
		}
	}
}

func TestFilter(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testFilter(t, testee)
		})
	}
}

func testPartition(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	partitionsQ, err := PartitionTo(testee.NewQueue(t), PartitionFunc(func(partition, v interface{}) (interface{}, error) {
		if partition == nil {
			partition = &[]int{}
		}

		p := partition.(*[]int)

		if len(*p) == 10 {
			p = &[]int{}
		}

		*p = append(*p, v.(int))

		return p, nil
	}))(nonNegIntsQ)
	if err != nil {
		t.Fatal(err)
	}

	partitionsCount := 0

	for {
		v, closed, err := partitionsQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case error:
			t.Fatal(v)
		case *[]int:
			if len(*v.(*[]int)) != 10 {
				t.Fatalf("len(v) is %d, want %d", len(*v.(*[]int)), 10)
			}

			partitionsCount++
		}
	}

	if partitionsCount != 100 {
		t.Fatalf("partitionsCount is %d, want 100", partitionsCount)
	}
}

func TestPartition(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testPartition(t, testee)
		})
	}
}

func testForkJoin(t *testing.T, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(t, testee, 1000)

	sum := ReduceFunc(func(acc, v interface{}) (interface{}, error) {
		if acc == nil {
			acc = 0
		}

		return acc.(int) + v.(int), nil
	})

	evenAndOddSumsQ, err := ForkJoinTo(
		testee.NewQueue(t),
		ForkJoinFunc(func(v interface{}) (map[int]interface{}, error) {
			if v.(int)%2 == 0 {
				return map[int]interface{}{0: v}, nil
			} else {
				return map[int]interface{}{1: v}, nil
			}
		}),
		ReduceTo(testee.NewQueue(t), sum),
		ReduceTo(testee.NewQueue(t), sum))(nonNegIntsQ)
	if err != nil {
		t.Fatal(err)
	}

	evenAndOddSum := 0

	for {
		v, closed, err := evenAndOddSumsQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		switch v.(type) {
		case error:
			t.Fatal(v)
		case int:
			if v.(int) != 249500 && v.(int) != 250000 {
				t.Fatalf("v is %d, want 249500 or 250000", sum)
			}

			evenAndOddSum += v.(int)
		}
	}

	if evenAndOddSum != 499500 {
		t.Fatalf("evenAndOddSum is %d, want 499500", sum)
	}
}

func TestForkJoin(t *testing.T) {
	for _, testee := range testees {
		t.Run(testee.Name, func(t *testing.T) {
			testForkJoin(t, testee)
		})
	}
}

func benchmarkQueue(b *testing.B, testee *testee) {
	nonNegIntsQ := nonNegIntsQ(b, testee, 100000)

	for i := 0; ; i++ {
		v, closed, err := nonNegIntsQ.Dequeue()
		if closed {
			break
		}
		if err != nil {
			b.Fatal(err)
		}

		switch v.(type) {
		case error:
			b.Fatal(v)
		case int:
			if v.(int) != i {
				b.Errorf("Dequeue => %d, want %d", v, i)
			}
		}
	}
}

func BenchmarkQueue(b *testing.B) {
	for _, testee := range testees {
		b.Run(testee.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkQueue(b, testee)
			}
		})
	}
}
