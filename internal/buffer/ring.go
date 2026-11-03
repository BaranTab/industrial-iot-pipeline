// Package buffer provides a bounded, thread-safe FIFO that drops the oldest
// item when full. The gateway uses it to keep readings while the broker is
// unreachable: losing the oldest data is better than running out of memory.
package buffer

import "sync"

// Ring is a fixed-capacity FIFO queue.
type Ring[T any] struct {
	mu      sync.Mutex
	items   []T
	head    int // index of the oldest item
	size    int // number of items stored
	dropped uint64
}

// New creates a ring that holds at most capacity items.
func New[T any](capacity int) *Ring[T] {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring[T]{items: make([]T, capacity)}
}

// Push adds v. If the ring is full the oldest item is overwritten and
// Push reports true.
func (r *Ring[T]) Push(v T) (dropped bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	capacity := len(r.items)
	if r.size == capacity {
		r.items[r.head] = v // overwrite the oldest
		r.head = (r.head + 1) % capacity
		r.dropped++
		return true
	}
	r.items[(r.head+r.size)%capacity] = v
	r.size++
	return false
}

// Peek returns the oldest item without removing it.
func (r *Ring[T]) Peek() (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero T
	if r.size == 0 {
		return zero, false
	}
	return r.items[r.head], true
}

// Pop removes and returns the oldest item.
func (r *Ring[T]) Pop() (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero T
	if r.size == 0 {
		return zero, false
	}
	v := r.items[r.head]
	r.items[r.head] = zero // let the GC reclaim it
	r.head = (r.head + 1) % len(r.items)
	r.size--
	return v, true
}

// Len returns the number of stored items.
func (r *Ring[T]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

// Dropped returns how many items were overwritten since creation.
func (r *Ring[T]) Dropped() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}
