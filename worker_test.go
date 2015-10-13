package workqueue

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestWorkerRegisterItselfToPool(t *testing.T) {
	RegisterTestingT(t)

	w := newWorker("1", func(j Job) {})

	workerpool := make(chan chan<- Job)
	defer close(workerpool)
	w.Start(workerpool)
	defer w.Stop()

	Eventually(workerpool).Should(Receive())
}

func TestWorkerPerformsJobs(t *testing.T) {
	RegisterTestingT(t)

	counter := 0
	w := newWorker("2", func(j Job) {
		counter++
	})

	workerpool := make(chan chan<- Job)
	defer close(workerpool)
	w.Start(workerpool)
	defer w.Stop()

	{
		workerC := <-workerpool
		workerC <- struct{}{}
		Eventually(func() int { return counter }).Should(Equal(1))
	}
	{
		workerC := <-workerpool
		workerC <- struct{}{}
		Eventually(func() int { return counter }).Should(Equal(2))
	}
	{
		workerC := <-workerpool
		workerC <- struct{}{}
		Eventually(func() int { return counter }).Should(Equal(3))
	}
}

func TestWorkerWorkerFnPanics(t *testing.T) {
	RegisterTestingT(t)

	w := newWorker("3", func(j Job) {
		panic("AAAA")
	})

	workerpool := make(chan chan<- Job)
	defer close(workerpool)
	w.Start(workerpool)
	defer w.Stop()

	{
		workerC := <-workerpool
		workerC <- struct{}{}
	}
}

func TestWorkerPoolIsClosed(t *testing.T) {
	RegisterTestingT(t)

	w := newWorker("4", func(j Job) {
		time.Sleep(10 * time.Millisecond)
	})

	workerpool := make(chan chan<- Job)

	go func() {
		/* Undefined behaviour. In some cases it would throw an
		 * exception when it will try to write to a workerpool channel
		 * that is closed at that moment. But in some execution
		 * scenarios it migth exit gracefully.
		 */
		defer func() {
			_ = recover()
		}()

		w.jobChannel = make(chan Job)
		w.done = make(chan struct{})
		w.loop(workerpool)
	}()

	workerC := <-workerpool
	close(workerpool)
	workerC <- struct{}{}

	// The sleep is needed to increase the probability of main worker loop to
	// panic
	time.Sleep(100 * time.Millisecond)
	w.Stop()
}
