package workqueue

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

const (
	// Pretty much a radndom number
	defNumWorkersPerCore = 8
)

type Job interface{}

type WorkQueueOptions struct {
	Id         string
	MaxWorkers int
	WorkerFn   func(Job)
}

type WorkQueue struct {
	Options WorkQueueOptions

	mutex    sync.Mutex
	started  bool
	stopping bool
	jobQueue jobQueue
	workers  []*worker
}

func New(id string, maxWorkers int, workerFn func(Job)) *WorkQueue {
	return NewWithOptions(&WorkQueueOptions{
		Id:         id,
		MaxWorkers: maxWorkers,
		WorkerFn:   workerFn,
	})
}

func NewWithOptions(options *WorkQueueOptions) *WorkQueue {
	q := &WorkQueue{
		Options: *options,
	}
	if q.Options.MaxWorkers <= 0 {
		ncores := runtime.GOMAXPROCS(-1)
		q.Options.MaxWorkers = defNumWorkersPerCore * ncores
	}
	return q
}

func (q *WorkQueue) Start() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.started {
		return QueueIsStarted
	}

	q.jobQueue = newJobListQueue()

	q.workers = make([]*worker, q.Options.MaxWorkers)
	for i := 0; i < len(q.workers); i++ {
		workerId := fmt.Sprintf("%s-worker-%d", q.Options.Id, i)
		worker := newWorker(workerId, q.Options.WorkerFn)
		q.workers[i] = worker
		worker.Start(q.jobQueue)
	}

	q.started = true

	return nil
}

func (q *WorkQueue) Stop(waitForPendingJobs bool) error {
	// Stop receiving new jobs
	_, err := q.withRunningQueue(func() (interface{}, error) {
		q.stopping = true
		return nil, nil
	})
	if err != nil {
		return err
	}

	if waitForPendingJobs {
		// XXX Monitor the progress and update sleep time accordingly.
		for !q.jobQueue.Empty() {
			time.Sleep(300 * time.Millisecond)
		}
	}

	// Stop the workers and the dispatcher
	q.jobQueue.AbortWaiters(true)
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Stop()
	}

	// Done
	_, _ = q.withSafeCtx(func() (interface{}, error) {
		q.started = false
		q.stopping = false
		return nil, nil
	})
	return nil
}

func (q *WorkQueue) Add(j Job) error {
	_, err := q.withRunningQueue(func() (interface{}, error) {
		q.jobQueue.Push(j)
		return nil, nil
	})

	return err
}

func (q *WorkQueue) withRunningQueue(
	fn func() (interface{}, error),
) (interface{}, error) {
	return q.withSafeCtx(func() (interface{}, error) {
		if !q.started {
			return nil, QueueNotStarted
		}
		if q.stopping {
			return nil, QueueIsStopping
		}

		return fn()
	})
}

func (q *WorkQueue) withSafeCtx(
	fn func() (interface{}, error),
) (interface{}, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return fn()
}
