package concurrent

import (
	"runtime"
	"sync"
	"sync/atomic"
)

func NewNochanPool(maxWorkerCnt int) Pool {
	if maxWorkerCnt <= 0 {
		maxWorkerCnt = runtime.NumCPU()
	}

	pool := &NoChanPool{
		wg:     new(sync.WaitGroup),
		tokens: make(chan interface{}, maxWorkerCnt),
	}
	pool.isShutDown = 0
	pool.AddMaxWorkerCnt(int64(maxWorkerCnt))

	for i := 0; i < maxWorkerCnt; i++ {
		pool.tokens <- struct{}{}
	}

	return pool
}

func (pool *NoChanPool) acquire() {
	<-pool.tokens
}

func (pool *NoChanPool) release() {
	pool.tokens <- 1
}

func (pool *NoChanPool) execute(t Task) {
	pool.wg.Add(1)
	go func() {
		pool.acquire()
		defer func() {
			pool.release()
			pool.wg.Done()
		}()
		runTask(t)
	}()
}

func (pool *NoChanPool) ShutDown() {
	if !atomic.CompareAndSwapInt32(&pool.isShutDown, 0, 1) {
		return
	}
	pool.wg.Wait()
}

func (pool *NoChanPool) Execute(t Task) {
	if t != nil {
		pool.execute(t)
	}
}

func (pool *NoChanPool) ExecuteFunc(f func() interface{}) {
	fw := &funcWrapper{
		f: f,
	}
	pool.Execute(fw)
}

func (pool *NoChanPool) Submit(t Task) (Future, error) {
	if t == nil {
		return nil, TaskInvalid
	}

	f := &FutureResult{}
	f.resultChan = make(chan interface{}, 1)
	tw := &taskWrapper{
		t: t,
		f: f,
	}

	pool.execute(tw)
	return f, nil
}

func (pool *NoChanPool) SubmitFunc(f func() interface{}) (Future, error) {
	fw := &funcWrapper{
		f: f,
	}
	return pool.Submit(fw)
}
