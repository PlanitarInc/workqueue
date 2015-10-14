package workqueue

import (
	"sync"
	"testing"

	. "github.com/onsi/gomega"
	set "gopkg.in/fatih/set.v0"
)

func TestJobQueueSimpleUseCase(t *testing.T) {
	RegisterTestingT(t)

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	// [1]
	q.Push(1)

	j, ok := q.Top()
	Ω(ok).Should(BeTrue())
	Ω(j).Should(Equal(1))

	// [1,2,3]
	q.Push(2)
	q.Push(3)

	j, ok = q.Top()
	Ω(ok).Should(BeTrue())
	Ω(j).Should(Equal(1))

	// [2,3]
	j, ok = q.Pop()
	Ω(ok).Should(BeTrue())
	Ω(j).Should(Equal(1))

	j, ok = q.Top()
	Ω(ok).Should(BeTrue())
	Ω(j).Should(Equal(2))
}

func TestJobQueueIsEmpty(t *testing.T) {
	RegisterTestingT(t)

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	j, ok := q.Top()
	Ω(ok).Should(BeFalse())
	Ω(j).Should(BeNil())

	j, ok = q.Pop()
	Ω(ok).Should(BeFalse())
	Ω(j).Should(BeNil())
}

func TestJobQueueEmpty(t *testing.T) {
	RegisterTestingT(t)

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	Ω(q.Empty()).Should(BeTrue())

	q.Push(1)
	Ω(q.Empty()).Should(BeFalse())

	_, _ = q.Top()
	Ω(q.Empty()).Should(BeFalse())

	_, _ = q.Pop()
	Ω(q.Empty()).Should(BeTrue())
}

func TestJobQueueWaitSyncPath(t *testing.T) {
	RegisterTestingT(t)

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	q.Push(1)

	j, ok := q.WaitForJob()
	Ω(j).Should(Equal(1))
	Ω(ok).Should(BeTrue())
}

func TestJobQueueWaitAsyncPath(t *testing.T) {
	RegisterTestingT(t)

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		j1, ok := q.WaitForJob()
		Ω(j1).Should(Equal(1))
		Ω(ok).Should(BeTrue())
		wg.Done()
		j2, ok := q.WaitForJob()
		Ω(j2).Should(Equal(2))
		Ω(ok).Should(BeTrue())
		wg.Done()
		j3, ok := q.WaitForJob()
		Ω(j3).Should(Equal(3))
		Ω(ok).Should(BeTrue())
		wg.Done()
	}()

	q.Push(1)
	q.Push(2)
	q.Push(3)

	wg.Wait()
}

func TestJobQueueAbortWaiters(t *testing.T) {
	RegisterTestingT(t)

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	waiterPending := sync.WaitGroup{}
	waiterFinished := sync.WaitGroup{}

	waiter := func() {
		waiterPending.Done()
		defer waiterFinished.Done()
		j, ok := q.WaitForJob()
		Ω(j).Should(BeNil())
		Ω(ok).Should(BeFalse())
	}

	waiterPending.Add(3)
	waiterFinished.Add(3)
	go waiter()
	go waiter()
	go waiter()

	waiterPending.Wait()
	q.AbortWaiters(true)
	q.AbortWaiters(false)
	waiterFinished.Wait() // Make sure Gomega tests are done

	waiterPending.Add(2)
	waiterFinished.Add(2)
	go waiter()
	go waiter()

	waiterPending.Wait()
	q.AbortWaiters(true)
	q.AbortWaiters(false)
	waiterFinished.Wait() // Make sure Gomega tests are done

	waiterPending.Add(1)
	waiterFinished.Add(1)
	go waiter()

	waiterPending.Wait()
	q.AbortWaiters(true)
	q.AbortWaiters(false)
	waiterFinished.Wait() // Make sure Gomega tests are done

	waiterPending.Wait()
	q.AbortWaiters(true)
	waiterFinished.Wait() // Make sure Gomega tests are done

	// The abortWaiters flag is still enabled
	waiterPending.Add(1)
	waiterFinished.Add(1)
	go waiter()

	waiterPending.Wait()
	q.AbortWaiters(false)
	waiterFinished.Wait() // Make sure Gomega tests are done
}

func TestJobQueuePushWaitStress(t *testing.T) {
	RegisterTestingT(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	pushed := set.New()
	popped := set.New()

	q := newJobListQueue()
	Ω(q).ShouldNot(BeNil())

	wg := sync.WaitGroup{}

	N := 50 * 1000
	wg.Add(N)

	for i := 0; i < N; i++ {
		pusher, item := i%2 == 0, i/2

		if pusher {
			go func(item int) {
				defer wg.Done()
				pushed.Add(item)
				q.Push(item)
			}(item)
		} else {
			go func() {
				defer wg.Done()
				item, ok := q.WaitForJob()
				Ω(ok).ShouldNot(BeFalse())
				popped.Add(item)
			}()
		}
	}

	wg.Wait()

	unpopped := set.Difference(pushed, popped)
	Ω(set.IntSlice(unpopped)).Should(BeEmpty())
	unpushed := set.Difference(popped, pushed)
	Ω(set.IntSlice(unpushed)).Should(BeEmpty())
	Ω(set.IntSlice(pushed)).Should(HaveLen(N / 2))
	Ω(set.IntSlice(popped)).Should(HaveLen(N / 2))
}

func BenchmarkJobQueuePush1(b *testing.B) {
	benchJobQueuePush(b, 1)
}

func BenchmarkJobQueuePush2(b *testing.B) {
	benchJobQueuePush(b, 2)
}

func BenchmarkJobQueuePush4(b *testing.B) {
	benchJobQueuePush(b, 4)
}

func BenchmarkJobQueuePush8(b *testing.B) {
	benchJobQueuePush(b, 8)
}

func BenchmarkJobQueuePush16(b *testing.B) {
	benchJobQueuePush(b, 16)
}

func BenchmarkJobQueuePush32(b *testing.B) {
	benchJobQueuePush(b, 32)
}

func BenchmarkJobQueuePush64(b *testing.B) {
	benchJobQueuePush(b, 64)
}

func benchJobQueuePush(b *testing.B, nroutines int) {
	q := newJobListQueue()
	wg := sync.WaitGroup{}

	wg.Add(nroutines)

	for routine := 0; routine < nroutines; routine++ {
		go func() {
			for i := 0; i < b.N; i++ {
				q.Push(i)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func BenchmarkJobQueuePushWait1(b *testing.B) {
	// The result should be similar to BenchmarkJobQueuePush1
	benchJobQueuePushWait(b, 1)
}

func BenchmarkJobQueuePushWait2(b *testing.B) {
	// The result should be similar to BenchmarkJobQueuePushWait1
	benchJobQueuePushWait(b, 2)
}

func BenchmarkJobQueuePushWait4(b *testing.B) {
	benchJobQueuePushWait(b, 4)
}

func BenchmarkJobQueuePushWait8(b *testing.B) {
	benchJobQueuePushWait(b, 8)
}

func BenchmarkJobQueuePushWait16(b *testing.B) {
	benchJobQueuePushWait(b, 16)
}

func BenchmarkJobQueuePushWait32(b *testing.B) {
	benchJobQueuePushWait(b, 32)
}

func BenchmarkJobQueuePushWait64(b *testing.B) {
	benchJobQueuePushWait(b, 64)
}

func BenchmarkJobQueuePushWait128(b *testing.B) {
	benchJobQueuePushWait(b, 128)
}

func benchJobQueuePushWait(b *testing.B, nroutines int) {
	q := newJobListQueue()
	wg := sync.WaitGroup{}

	wg.Add(nroutines)

	for routine := 0; routine < nroutines; routine++ {
		pusher := routine%3 < 2
		go func(pusher bool) {
			for i := 0; i < b.N; i++ {
				if pusher {
					q.Push(i)
				} else {
					q.WaitForJob()
				}
			}
			wg.Done()
		}(pusher)
	}

	wg.Wait()
}
