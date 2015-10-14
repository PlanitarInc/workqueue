package workqueue

import (
	"fmt"
	"runtime/debug"
	"time"
)

type worker struct {
	Id       string
	WorkerFn func(Job)
	quit     chan struct{}
}

func newWorker(id string, workerFn func(Job)) *worker {
	return &worker{
		Id:       id,
		WorkerFn: workerFn,
	}
}

func (w *worker) loop(jq jobQueue) {
	for {
		select {
		case <-w.quit:
			return

		default:
			if job, ok := jq.WaitForJob(); ok {
				w.execute(job)
			} else {
				time.Sleep(5 * time.Millisecond) // XXX
			}
		}
	}
}

func (w worker) execute(j Job) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("worker-%s: worker fn paniced: %s: %s\n", w.Id, r, debug.Stack())
		}
	}()

	w.WorkerFn(j)
}

func (w *worker) Start(jq jobQueue) {
	w.quit = make(chan struct{})
	go w.loop(jq)
}

// The method blocks until the waiters of worker's job queue are aborted
func (w *worker) Stop() {
	w.quit <- struct{}{}
}
