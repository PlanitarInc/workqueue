package workqueue

import (
	"fmt"
	"sync"
)

type Job interface{}

type WorkQueueOptions struct {
	Id         string
	MaxWorkers int
	WorkerFn   func(Job)
}

type WorkQueue struct {
	Options WorkQueueOptions

	mutex      sync.Mutex
	started    bool
	stopping   bool
	jobQueue   chan Job
	dispatcher *dispatcher
	workers    []*worker
}

func New(id string, maxWorkers int, workerFn func(Job)) *WorkQueue {
	return &WorkQueue{
		Options: WorkQueueOptions{
			MaxWorkers: maxWorkers,
			WorkerFn:   workerFn,
		},
	}
}

func (q *WorkQueue) Start() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.started {
		return QueueIsStarted
	}

	q.jobQueue = make(chan Job)

	dispatcherId := fmt.Sprintf("%s-dispatcher", q.Options.Id)
	q.dispatcher = newDispatcher(dispatcherId, q.Options.MaxWorkers)
	workerPool := q.dispatcher.Start(q.jobQueue)

	q.workers = make([]*worker, q.Options.MaxWorkers)
	for i := 0; i < len(q.workers); i++ {
		workerId := fmt.Sprintf("%s-worker-%d", q.Options.Id, i)
		worker := newWorker(workerId, q.Options.WorkerFn)
		q.workers[i] = worker
		worker.Start(workerPool)
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
		q.dispatcher.Drain()
	}

	// Stop the workers and the dispatcher
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Stop()
	}
	q.dispatcher.Stop()
	// Now it's safe to close the job queue channel
	close(q.jobQueue)

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
		q.jobQueue <- j
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
