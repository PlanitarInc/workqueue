package workqueue

import (
	"runtime"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	set "gopkg.in/fatih/set.v0"
)

func TestQueueStartAndStop(t *testing.T) {
	RegisterTestingT(t)

	q := New("test", 1, func(j Job) {})
	Ω(q.Start()).Should(BeNil())
	Ω(q.Stop(true)).Should(BeNil())
}

func TestQueueStop(t *testing.T) {
	RegisterTestingT(t)

	jobsDone := []int{}
	q := New("stop", 1, func(j Job) {
		time.Sleep(10 * time.Millisecond)
		jobsDone = append(jobsDone, j.(int))
	})

	Ω(q.Start()).Should(BeNil())

	Ω(q.Add(1))
	Ω(q.Add(2))
	Ω(q.Add(3))
	Ω(q.Add(4))
	Ω(q.Add(5))

	Ω(q.Stop(true)).Should(BeNil())

	Ω(jobsDone).Should(Equal([]int{1, 2, 3, 4, 5}))
}

func TestQueueStopNoWait(t *testing.T) {
	RegisterTestingT(t)

	jobsDone := []int{}
	q := New("stop-no-wait", 1, func(j Job) {
		time.Sleep(100 * time.Millisecond)
		jobsDone = append(jobsDone, j.(int))
	})

	Ω(q.Start()).Should(BeNil())

	Ω(q.Add(1))
	Ω(q.Add(2))
	Ω(q.Add(3))
	Ω(q.Add(4))
	Ω(q.Add(5))

	Ω(q.Stop(false)).Should(BeNil())

	Ω(len(jobsDone) < 10).Should(BeTrue())
	Ω(q.jobQueue.Empty()).Should(BeFalse())
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

func TestQueueMaxWorkers(t *testing.T) {
	RegisterTestingT(t)

	original := runtime.GOMAXPROCS(-1)
	defer runtime.GOMAXPROCS(original)

	{
		runtime.GOMAXPROCS(1)
		q := New("", 0, func(j Job) {})
		Ω(q.Options.MaxWorkers).Should(Equal(defNumWorkersPerCore))
	}

	{
		runtime.GOMAXPROCS(2)
		q := New("", 0, func(j Job) {})
		Ω(q.Options.MaxWorkers).Should(Equal(2 * defNumWorkersPerCore))
	}

	{
		runtime.GOMAXPROCS(3)
		q := New("", 0, func(j Job) {})
		Ω(q.Options.MaxWorkers).Should(Equal(3 * defNumWorkersPerCore))
	}

	{
		q := New("", 9, func(j Job) {})
		Ω(q.Options.MaxWorkers).Should(Equal(9))
	}

	{
		q := New("", 1, func(j Job) {})
		Ω(q.Options.MaxWorkers).Should(Equal(1))
	}

	{
		q := New("", 10000, func(j Job) {})
		Ω(q.Options.MaxWorkers).Should(Equal(10000))
	}
}

func TestQueueStress(t *testing.T) {
	RegisterTestingT(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	addedJobs := set.New()
	processedJobs := set.New()

	q := New("stress", 100, func(j Job) {
		time.Sleep(3 * time.Millisecond)
		processedJobs.Add(j)
	})

	Ω(q.Start()).Should(BeNil())

	N := 5 * 1000
	wg := sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(item int) {
			defer wg.Done()
			addedJobs.Add(item)
			Ω(q.Add(item)).Should(BeNil())
		}(i)
	}
	wg.Wait() // Wait while all jobs are added to the queue

	Ω(q.Stop(true)).Should(BeNil())

	unprocessed := set.Difference(addedJobs, processedJobs)
	Ω(set.IntSlice(unprocessed)).Should(BeEmpty())
	unadded := set.Difference(processedJobs, addedJobs)
	Ω(set.IntSlice(unadded)).Should(BeEmpty())
	Ω(set.IntSlice(addedJobs)).Should(HaveLen(N))
	Ω(set.IntSlice(processedJobs)).Should(HaveLen(N))
}

func BenchmarkQueueAdd1(b *testing.B) {
	benchQueueAdd(b, 1)
}

func BenchmarkQueueAdd2(b *testing.B) {
	benchQueueAdd(b, 2)
}

func BenchmarkQueueAdd4(b *testing.B) {
	benchQueueAdd(b, 4)
}

func BenchmarkQueueAdd8(b *testing.B) {
	benchQueueAdd(b, 8)
}

func BenchmarkQueueAdd16(b *testing.B) {
	benchQueueAdd(b, 16)
}

func BenchmarkQueueAdd32(b *testing.B) {
	benchQueueAdd(b, 32)
}

func BenchmarkQueueAdd64(b *testing.B) {
	benchQueueAdd(b, 64)
}

func BenchmarkQueueAdd128(b *testing.B) {
	benchQueueAdd(b, 128)
}

func benchQueueAdd(b *testing.B, nworkers int) {
	q := New("bench", nworkers, func(j Job) {})
	q.Start()

	wg := sync.WaitGroup{}
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			q.Add(i)
		}()
	}
	wg.Wait()

	q.Stop(true)
}
