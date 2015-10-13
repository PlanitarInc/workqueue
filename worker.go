package workqueue

import "fmt"

type worker struct {
	Id         string
	WorkerFn   func(Job)
	jobChannel chan Job
	done       chan struct{}
	running    bool
}

func newWorker(id string, workerFn func(Job)) *worker {
	return &worker{
		Id:       id,
		WorkerFn: workerFn,
	}
}

func (w *worker) loop(workerPool chan<- chan<- Job) {
	defer func() {
		close(w.done)
	}()

	for {
		fmt.Println("worker %s submitting job channel", w.Id)
		select {
		case workerPool <- w.jobChannel:
			fmt.Println("worker %s waiting for a job", w.Id)
			select {
			case job := <-w.jobChannel:
				fmt.Println("worker %s got a job", w.Id)
				w.execute(job)

			case <-w.done:
				fmt.Printf("worker-%s: is done\n", w.Id)
				return
			}

		case <-w.done:
			fmt.Printf("worker-%s: is done\n", w.Id)
			return
		}
	}
}

func (w worker) execute(j Job) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("worker-%s: worker fn paniced: %s\n", w.Id, r)
		}
	}()

	w.WorkerFn(j)
}

func (w *worker) Start(workerPool chan<- chan<- Job) {
	w.jobChannel = make(chan Job)
	w.done = make(chan struct{})

	go w.loop(workerPool)
}

func (w *worker) Stop() {
	w.stopLoop()
	close(w.jobChannel)
}

func (w *worker) stopLoop() {
	// If the main loop is still running, we get blocked until it reads from
	// the done channel.
	// If the main loop is not running, w.done object should be closed and
	// our code will panic.
	defer func() {
		fmt.Println("worker %s paniced during stopLoop()", w.Id)
		_ = recover()
	}()
	w.done <- struct{}{}
	fmt.Println("worker %s stopped a loop OK", w.Id)
}
