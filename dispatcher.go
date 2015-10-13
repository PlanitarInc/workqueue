package workqueue

import (
	"fmt"
	"sync"
)

type dispatcher struct {
	Id             string
	WorkerPoolSize int
	done           chan struct{}
	workerPool     chan chan<- Job
	pendingJobs    sync.WaitGroup
}

func newDispatcher(id string, workerPoolSize int) *dispatcher {
	return &dispatcher{
		Id:             id,
		WorkerPoolSize: workerPoolSize,
	}
}

func (d *dispatcher) loop(jobQueue <-chan Job) {
	defer func() {
		close(d.done)
	}()

	for {
		select {
		case job := <-jobQueue:

			fmt.Println("disp got job", job)
			// XXX Use channel buffering instead of spawning a gorouting for
			// every new job.
			// Pros:
			//   - the order of jobs is maintained
			//   - the queue size is not unlimited
			// Cons:
			//   - the queue size is limited
			//   - ?
			// See also https://groups.google.com/forum/#!topic/golang-nuts/UnzE5vgyzqw
			d.pendingJobs.Add(1)
			go func(job Job) {
				defer d.pendingJobs.Done()
				fmt.Println("disp waiting to assign job", job)
				jobChannel, ok := <-d.workerPool
				if !ok {
					fmt.Println("disp aborting job", job)
					return
				}

				defer func() {
					_ = recover()
				}()
				fmt.Println("disp got worker channel, assigning job", job)
				jobChannel <- job
				fmt.Println("disp sent job to worker", job)
			}(job)

		case <-d.done:
			fmt.Printf("dispatcher-%s is done\n", d.Id)
			return
		}
	}
}

func (d *dispatcher) Start(jobQueue <-chan Job) chan<- chan<- Job {
	d.done = make(chan struct{})
	d.workerPool = make(chan chan<- Job, d.WorkerPoolSize)

	go d.loop(jobQueue)

	return d.workerPool
}

func (d *dispatcher) Stop() {
	d.stopLoop()
	close(d.workerPool)
	d.pendingJobs.Wait()
}

func (d *dispatcher) Drain() {
	d.pendingJobs.Wait()
}

func (d *dispatcher) stopLoop() {
	// If the main loop is still running, we get blocked until it reads from
	// the done channel.
	// If the main loop is not running, w.done object should be closed and
	// our code will panic.
	defer func() {
		_ = recover()
	}()
	d.done <- struct{}{}
}
