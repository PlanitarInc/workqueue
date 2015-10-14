package workqueue

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestWorkerPerformsJobs(t *testing.T) {
	RegisterTestingT(t)

	counter := 0
	w := newWorker("perform", func(j Job) {
		counter++
	})

	jq := newJobListQueue()
	w.Start(jq)
	defer func() {
		jq.AbortWaiters(true)
		w.Stop()
	}()

	{
		jq.Push(struct{}{})
		Eventually(func() int { return counter }).Should(Equal(1))
	}
	{
		jq.Push(struct{}{})
		Eventually(func() int { return counter }).Should(Equal(2))
	}
	{
		jq.Push(struct{}{})
		Eventually(func() int { return counter }).Should(Equal(3))
	}
}

func TestWorkerStop(t *testing.T) {
	RegisterTestingT(t)

	counter := 0
	w := newWorker("stop", func(j Job) {
		counter++
	})

	jq := newJobListQueue()
	w.Start(jq)

	jq.AbortWaiters(true)
	w.Stop()

	jq.Push(struct{}{})
	Eventually(func() int { return counter }).ShouldNot(Equal(1))
	Eventually(func() bool { return jq.Empty() }).ShouldNot(BeTrue())
}

func TestWorkerWorkerFnPanics(t *testing.T) {
	RegisterTestingT(t)

	w := newWorker("panic", func(j Job) {
		panic("AAAA")
	})

	jq := newJobListQueue()
	w.Start(jq)
	defer func() {
		jq.AbortWaiters(true)
		w.Stop()
	}()

	jq.Push(struct{}{})
	Eventually(func() bool { return jq.Empty() }).Should(BeTrue())
}

func TestWorkerJobQueueWaitingIsAbortedThenResumed(t *testing.T) {
	RegisterTestingT(t)

	counter := 0
	w := newWorker("aborted-resumed", func(j Job) {
		time.Sleep(3 * time.Millisecond)
		counter++
	})

	jq := newJobListQueue()
	w.Start(jq)
	defer func() {
		jq.AbortWaiters(true)
		w.Stop()
	}()

	jq.Push(struct{}{})
	Eventually(func() int { return counter }).Should(Equal(1))
	Eventually(func() bool { return jq.Empty() }).Should(BeTrue())

	jq.AbortWaiters(true)
	jq.Push(struct{}{})
	jq.Push(struct{}{})
	time.Sleep(10 * time.Millisecond)
	Eventually(func() int { return counter }).Should(Equal(1))
	Eventually(func() bool { return jq.Empty() }).Should(BeFalse())

	jq.AbortWaiters(false)
	Eventually(func() int { return counter }).Should(Equal(3))
	Eventually(func() bool { return jq.Empty() }).Should(BeTrue())
}
