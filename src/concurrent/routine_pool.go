package concurrent

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var SubmitTimeoutError = errors.New("Submit task timeout!")
var PoolShutDownError = errors.New("RoutinePool is shutdown!")
var TaskRejectError = errors.New("Submit task is rejected!")

var closeQueue = signalTask{id: "closeQueue"}

func NewRoutinePool(maxWorkerCnt, cacheCnt int) Pool {
	if maxWorkerCnt <= 0 {
		maxWorkerCnt = runtime.NumCPU()
	}

	pool := &RoutinePool{
		cacheCnt:   cacheCnt,
		wg:         new(sync.WaitGroup),
		lock:       new(sync.Mutex),
		shutDownWg: new(sync.WaitGroup),
		autoTune:   false,
	}
	pool.isShutDown = 0
	pool.basicPool.AddMaxWorkerCnt(int64(maxWorkerCnt))
	if pool.cacheCnt <= 0 {
		pool.taskQueue = make(chan Task)
	} else {
		pool.taskQueue = make(chan Task, pool.cacheCnt)
	}
	pool.workers = make(map[string]*worker, pool.maxWorkerCnt)
	// dispatchQueue must not have length
	pool.dispatchQueue = make(chan Task)
	pool.dispatcher()

	return pool
}

func (pool *RoutinePool) checkStatus(t Task) error {
	if t == nil {
		return TaskInvalid
	}

	if atomic.LoadInt32(&pool.isShutDown) == 1 {
		return PoolShutDownError
	}
	return nil
}

func (pool *RoutinePool) dispatcher() {
	pool.shutDownWg.Add(1)
	go func() {
		for {
			task, ok := <-pool.dispatchQueue
			if !ok {
				break
			}

			if task == closeQueue {
				close(pool.taskQueue)
				pool.shutDownWg.Done()
				continue
			}

			if pool.GetWorkerCnt() < pool.GetMaxWorkerCnt() {
				pool.addWorker()
			}

			pool.taskQueue <- task
		}
	}()
}

func (pool *RoutinePool) AddMaxWorkerCnt(value int64) int64 {
	if pool.autoTune {
		return pool.basicPool.AddMaxWorkerCnt(value)
	}
	return pool.GetMaxWorkerCnt()
}

func (pool *RoutinePool) addWorker() {
	if pool.autoTune {
		pool.lock.Lock()
		defer pool.lock.Unlock()
	}
	w := &worker{}
	w.name = fmt.Sprintf("woker-%d", len(pool.workers))
	w.taskQueue = pool.taskQueue
	w.wg = pool.wg
	pool.AddWorkerCnt(1)
	w.pool = pool
	pool.workers[w.name] = w
	pool.wg.Add(1)
	w.start()
}

func (pool *RoutinePool) autoTuneWorker(w *worker) bool {
	if !pool.autoTune {
		return false
	}

	if w == nil {
		return false
	}

	workerCnt := pool.GetWorkerCnt()
	maxWorkerCnt := pool.GetMaxWorkerCnt()
	if workerCnt > maxWorkerCnt && atomic.CompareAndSwapInt64(&pool.workerCnt, workerCnt, workerCnt-1) {
		pool.lock.Lock()
		defer pool.lock.Unlock()
		delete(pool.workers, w.name)
		w.wg.Done()
		w.release()
	}

	return false
}

func (pool *RoutinePool) ExecuteFunc(f func() interface{}) {
	fw := &funcWrapper{
		f: f,
	}
	pool.Execute(fw)
}

func (pool *RoutinePool) Execute(t Task) {
	if t != nil {
		pool.dispatchQueue <- t
	}
}

func (pool *RoutinePool) SubmitFunc(f func() interface{}) (Future, error) {
	fw := &funcWrapper{
		f: f,
	}
	return pool.Submit(fw)
}

func (pool *RoutinePool) Submit(t Task) (Future, error) {
	if err := pool.checkStatus(t); err != nil {
		return nil, err
	}
	f := &FutureResult{}
	f.resultChan = make(chan interface{}, 1)
	tw := &taskWrapper{
		t: t,
		f: f,
	}
	pool.dispatchQueue <- tw
	return f, nil
}

func (pool *RoutinePool) SubmitWithTimeout(t Task, timeout int64) (Future, error) {
	if timeout <= 0 {
		return pool.Submit(t)
	}
	if err := pool.checkStatus(t); err != nil {
		return nil, err
	}
	timeoutChan := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(time.Millisecond * time.Duration(timeout)))
		timeoutChan <- true
		close(timeoutChan)
	}()

	f := &FutureResult{}
	f.resultChan = make(chan interface{}, 1)
	tw := &taskWrapper{
		t: t,
		f: f,
	}
	select {
	case pool.dispatchQueue <- tw:
		return f, nil
	case <-timeoutChan:
		return nil, SubmitTimeoutError
	}
}

func (pool *RoutinePool) beforeCloseDispatchQueue() {
	if !atomic.CompareAndSwapInt32(&pool.isShutDown, 0, 1) {
		return
	}
	pool.dispatchQueue <- closeQueue
	pool.wg.Wait()
}

func (pool *RoutinePool) doCloseDispatchQueue() {
	close(pool.dispatchQueue)
	pool.shutDownWg.Wait()
}

func (pool *RoutinePool) ShutDown() {
	pool.beforeCloseDispatchQueue()
	pool.doCloseDispatchQueue()
	for _, w := range pool.workers {
		w.release()
	}
	pool.workers = nil
	pool.taskQueue = nil
	pool.dispatchQueue = nil
}
