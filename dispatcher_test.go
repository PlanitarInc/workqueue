package workqueue

import (
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
)

func TestDispatcherForwardsJobsToWorkerChanel(t *testing.T) {
	RegisterTestingT(t)

	jobQueue := make(chan Job)
	defer close(jobQueue)
	d := newDispatcher("d1", 1)
	workerpool := d.Start(jobQueue)
	defer d.Stop()

	worker1 := make(chan Job)
	defer close(worker1)

	workerpool <- worker1
	jobQueue <- 1
	Eventually(worker1).Should(Receive(Equal(1)))
}

func TestDispatcherForwardsJobsToMultipleWorkerChanels(t *testing.T) {
	RegisterTestingT(t)

	jobQueue := make(chan Job)
	defer close(jobQueue)
	d := newDispatcher("d2", 2)
	workerpool := d.Start(jobQueue)
	defer d.Stop()

	worker1 := make(chan Job)
	defer close(worker1)
	worker2 := make(chan Job)
	defer close(worker2)

	workerpool <- worker1
	jobQueue <- 1
	Eventually(worker1).Should(Receive(Equal(1)))

	workerpool <- worker2
	jobQueue <- 2
	Eventually(worker2).Should(Receive(Equal(2)))

	// Although go channels maintain the message order,
	// the dispatcher does not guarantee it.
	workerpool <- worker1
	workerpool <- worker2
	jobQueue <- 3 // msg1
	jobQueue <- 3 // msg2
	Eventually(worker1).Should(Receive(Equal(3)))
	Eventually(worker2).Should(Receive(Equal(3)))
}

func TestDispatcherCloseAbortsPendingJobs(t *testing.T) {
	RegisterTestingT(t)

	fmt.Println()
	fmt.Println()

	jobQueue := make(chan Job)
	defer close(jobQueue)
	d := newDispatcher("d3", 1)
	_ = d.Start(jobQueue)
	defer d.Stop()

	worker1 := make(chan Job)
	defer close(worker1)

	jobQueue <- 1
	jobQueue <- 2
	jobQueue <- 3
	jobQueue <- 4
}

func TestDispatcherDrain(t *testing.T) {
	RegisterTestingT(t)

	fmt.Println()
	fmt.Println()

	jobQueue := make(chan Job)
	defer close(jobQueue)
	d := newDispatcher("d4", 1)
	workerpool := d.Start(jobQueue)
	defer d.Stop()

	worker1 := make(chan Job)
	defer close(worker1)

	// Although go channels maintain the message order,
	// the dispatcher does not guarantee it.
	jobQueue <- 1
	jobQueue <- 1
	jobQueue <- 1
	jobQueue <- 1

	_ = workerpool

	go func() {
		workerpool <- worker1
		Eventually(worker1).Should(Receive(Equal(1)))
		workerpool <- worker1
		Eventually(worker1).Should(Receive(Equal(1)))
		workerpool <- worker1
		Eventually(worker1).Should(Receive(Equal(1)))
		workerpool <- worker1
		Eventually(worker1).Should(Receive(Equal(1)))
	}()

	d.Drain()
}
