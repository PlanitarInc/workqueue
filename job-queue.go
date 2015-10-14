package workqueue

import (
	"container/list"
	"sync"

	"golang.org/x/net/context"
)

type jobQueue interface {
	Top() (Job, bool)
	Pop() (Job, bool)
	Push(job Job)
	Empty() bool
	WaitForJob() (Job, bool)
	AbortWaiters(bool)
}

func jobQueueChan(jq jobQueue) (<-chan Job, context.CancelFunc) {
	return nil, nil
}

type jobListQueue struct {
	pending          *list.List
	mutex            *sync.Mutex
	newElemenentCond *sync.Cond
	abortWaiters     bool
	waitersWg        sync.WaitGroup
}

func newJobListQueue() *jobListQueue {
	m := &sync.Mutex{}

	return &jobListQueue{
		pending:          list.New(),
		mutex:            m,
		newElemenentCond: sync.NewCond(m),
	}
}

func (q *jobListQueue) Top() (Job, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if el := q.pending.Front(); el == nil {
		return nil, false
	} else {
		return el.Value.(Job), true
	}
}

func (q *jobListQueue) Pop() (Job, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.pop()
}

func (q *jobListQueue) Push(job Job) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.pending.PushBack(job)
	q.newElemenentCond.Signal()
}

func (q *jobListQueue) Empty() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.empty()
}

func (q jobListQueue) empty() bool {
	return q.pending.Len() == 0
}

func (q *jobListQueue) pop() (Job, bool) {
	if el := q.pending.Front(); el == nil {
		return nil, false
	} else {
		return q.pending.Remove(el).(Job), true
	}
}

func (q *jobListQueue) WaitForJob() (Job, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Avoid affecting waiters wait group
	if q.abortWaiters {
		return nil, false
	}

	q.waitersWg.Add(1)
	defer q.waitersWg.Done()

	for !q.abortWaiters && q.empty() {
		q.newElemenentCond.Wait()
	}
	if q.abortWaiters {
		return nil, false
	}

	// Queue is guaranteed to be non-empty here
	job, _ := q.pop()
	return job, true
}

func (q *jobListQueue) AbortWaiters(abortWaiters bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if abortWaiters == q.abortWaiters {
		// Nothing to do
		return
	}

	if !abortWaiters && q.abortWaiters {
		// Just set the flag
		q.abortWaiters = false
		return
	}

	// Wake up all current waiters
	q.abortWaiters = true
	q.newElemenentCond.Broadcast()

	// Wait for all waiters to abort
	q.mutex.Unlock()
	q.waitersWg.Wait()
	q.mutex.Lock()
}
