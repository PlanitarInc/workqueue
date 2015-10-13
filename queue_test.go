package workqueue

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestQueueStartAndStop(t *testing.T) {
	RegisterTestingT(t)

	q := New("test", 1, func(j Job) {})
	Ω(q.Start()).Should(BeNil())
	Ω(q.Stop(true)).Should(BeNil())
}

func TestQueueStop(t *testing.T) {
	RegisterTestingT(t)

	jobsDone := map[int]struct{}{}
	q := New("stop", 1, func(j Job) {
		time.Sleep(10 * time.Millisecond)
		jobsDone[j.(int)] = struct{}{}
	})

	Ω(q.Start()).Should(BeNil())

	Ω(q.Add(1))
	Ω(q.Add(2))
	Ω(q.Add(3))
	Ω(q.Add(4))
	Ω(q.Add(5))

	Ω(q.Stop(true)).Should(BeNil())

	Ω(jobsDone).Should(HaveKey(1))
	Ω(jobsDone).Should(HaveKey(2))
	Ω(jobsDone).Should(HaveKey(3))
	Ω(jobsDone).Should(HaveKey(4))
	Ω(jobsDone).Should(HaveKey(5))
}

func TestQueueStopNoWait(t *testing.T) {
	RegisterTestingT(t)

	jobsDone := map[int]struct{}{}
	q := New("stop-no-wait", 1, func(j Job) {
		time.Sleep(100 * time.Millisecond)
		jobsDone[j.(int)] = struct{}{}
	})

	Ω(q.Start()).Should(BeNil())

	for i := 0; i < 10; i++ {
		Ω(q.Add(i))
	}

	Ω(q.Stop(false)).Should(BeNil())

	Ω(len(jobsDone) < 10).Should(BeTrue())
}

func TestQueueAddToNotStartedQueue(t *testing.T) {
	RegisterTestingT(t)

	q := New("not-started", 1, func(j Job) {})

	Ω(q.Add(1)).Should(Equal(QueueNotStarted))
}

func TestQueueAddToStopping(t *testing.T) {
	RegisterTestingT(t)

	q := New("stopping", 1, func(j Job) {})

	Ω(q.Start()).Should(BeNil())
	q.stopping = true
	defer func() {
		q.stopping = false
		Ω(q.Stop(true)).Should(BeNil())
	}()

	Ω(q.Add(100)).Should(Equal(QueueIsStopping))
}

func TestQueueAddToStopped(t *testing.T) {
	RegisterTestingT(t)

	q := New("stopped", 1, func(j Job) {})

	Ω(q.Start()).Should(BeNil())
	Ω(q.Stop(true)).Should(BeNil())
	Ω(q.Add(100)).Should(Equal(QueueNotStarted))
}

func TestQueueStartStarted(t *testing.T) {
	RegisterTestingT(t)

	q := New("started", 1, func(j Job) {})

	Ω(q.Start()).Should(BeNil())
	Ω(q.Start()).Should(Equal(QueueIsStarted))
	Ω(q.Stop(true)).Should(BeNil())
}

func TestQueueStopStopped(t *testing.T) {
	RegisterTestingT(t)

	q := New("stopped", 1, func(j Job) {})

	Ω(q.Start()).Should(BeNil())
	Ω(q.Stop(true)).Should(BeNil())
	Ω(q.Stop(true)).Should(Equal(QueueNotStarted))
}
