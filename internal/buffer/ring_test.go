package buffer

import (
	"sync"
	"testing"
)

func TestRingFIFO(t *testing.T) {
	r := New[int](3)
	for i := 1; i <= 3; i++ {
		if r.Push(i) {
			t.Fatalf("push %d: unexpected drop", i)
		}
	}
	for want := 1; want <= 3; want++ {
		got, ok := r.Pop()
		if !ok || got != want {
			t.Fatalf("pop: got %d,%v want %d", got, ok, want)
		}
	}
	if _, ok := r.Pop(); ok {
		t.Fatal("pop on empty ring returned ok")
	}
}

func TestRingDropsOldestWhenFull(t *testing.T) {
	r := New[int](3)
	for i := 1; i <= 5; i++ { // 1 and 2 must be dropped
		r.Push(i)
	}
	if r.Len() != 3 {
		t.Fatalf("len: got %d want 3", r.Len())
	}
	if r.Dropped() != 2 {
		t.Fatalf("dropped: got %d want 2", r.Dropped())
	}
	if v, _ := r.Peek(); v != 3 {
		t.Fatalf("peek: got %d want 3", v)
	}
	for _, want := range []int{3, 4, 5} {
		if got, _ := r.Pop(); got != want {
			t.Fatalf("pop: got %d want %d", got, want)
		}
	}
}

func TestRingConcurrentPush(t *testing.T) {
	r := New[int](1000)
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.Push(i)
			}
		}()
	}
	wg.Wait()
	if r.Len() != 1000 {
		t.Fatalf("len: got %d want 1000", r.Len())
	}
}
