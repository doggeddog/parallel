package parallel

import (
	"fmt"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	var testData = make([]int, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = i
	}

	err := Batch(97, -1, testData[:], func(batch []int) error {
		fmt.Printf("size:%v, %#v\n", len(batch), batch)
		time.Sleep(time.Second)
		return nil
	})

	println(err)
}

func TestParallel(t *testing.T) {
	var testData = make([]int, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = i
	}

	results, err := Parallel(47, -1, testData[:], func(batch []int) (results []int, err error) {
		fmt.Printf("size:%v, %#v\n", len(batch), batch)
		time.Sleep(time.Second)
		for _, v := range batch {
			results = append(results, v*1000)
		}
		return results, nil
	})

	fmt.Printf("size:%v\n, results:%#v, %v\n", len(results), results, err)
}
