package parallel

import (
	"log"
	"runtime"
	"sync"
)

// Batch
// Batch 跟 Parallel功能类似
// 区别在于Batch 不返回结果, 需要用户在f函数中自己处理结果
func Batch[T any](routineNum, minBatchSize int, data []T, f func(batch []T) error) (err error) {
	dataSize := len(data)
	batchSize := calcActualBatchSize(dataSize, minBatchSize, routineNum)
	if batchSize <= 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	for begin := 0; begin < dataSize; begin += batchSize {
		end := 0
		if begin+batchSize > dataSize {
			end = dataSize
		} else {
			end = begin + batchSize
		}

		wg.Add(1)

		go func(begin, end int) {
			defer func() {
				if e := recover(); e != nil {
					stackBuf := make([]byte, 1<<20)
					_ = runtime.Stack(stackBuf, false)
					log.Printf("_panic_recover||error=%v||stack=%s", e, stackBuf)
				}
			}()

			defer wg.Done()
			err1 := f(data[begin:end])
			if err == nil && err1 != nil {
				err = err1
			}
		}(begin, end)

	}
	wg.Wait()
	return err
}

// Parallel
// Batch 跟 Parallel功能类似
// 区别在于Batch 不返回结果, 需要用户在f函数中自己处理结果
func Parallel[T any, R any](routineNum, minBatchSize int, data []T, f func(batch []T) (results []R, err error)) (results []R, err error) {
	dataSize := len(data)
	batchSize := calcActualBatchSize(dataSize, minBatchSize, routineNum)
	if batchSize <= 0 {
		return nil, nil
	}

	wg := sync.WaitGroup{}
	resultMutex := sync.Mutex{}
	for begin := 0; begin < dataSize; begin += batchSize {
		end := 0
		if begin+batchSize > dataSize {
			end = dataSize
		} else {
			end = begin + batchSize
		}

		wg.Add(1)

		go func(begin, end int) {
			defer func() {
				if e := recover(); e != nil {
					stackBuf := make([]byte, 1<<20)
					_ = runtime.Stack(stackBuf, false)
					log.Printf("_panic_recover||error=%v||stack=%s", e, stackBuf)
				}
			}()
			defer wg.Done()
			r, err1 := f(data[begin:end])
			if err == nil && err1 != nil {
				err = err1
			}
			resultMutex.Lock()
			results = append(results, r...)
			resultMutex.Unlock()
		}(begin, end)

	}
	wg.Wait()
	return results, err
}

func calcActualBatchSize(dataSize, minBatchSize int, routineNum int) int {
	if dataSize == 0 {
		return -1
	}
	if routineNum <= 0 {
		routineNum = 1
	}
	if minBatchSize <= 0 {
		minBatchSize = 1
	}

	batchSize := 0
	// 向上取整
	if dataSize%routineNum == 0 {
		batchSize = dataSize / routineNum
	} else {
		batchSize = dataSize/routineNum + 1
	}
	if batchSize < minBatchSize {
		batchSize = minBatchSize
	}
	return batchSize
}
