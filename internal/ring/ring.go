package ring

import "time"

type Entry[T any] struct {
	Value    T
	Enqueued int64
}

type Ring[T any] struct {
	items []Entry[T]
	head  int
	tail  int
	len   int
}

func New[T any](capacity int) *Ring[T] {
	if capacity <= 0 {
		panic("ring: capacity must be positive")
	}
	return &Ring[T]{
		items: make([]Entry[T], capacity),
	}
}

func (r *Ring[T]) Len() int {
	return r.len
}

func (r *Ring[T]) Cap() int {
	return len(r.items)
}

func (r *Ring[T]) Push(v T, now int64) bool {
	if r.len == len(r.items) {
		return false
	}
	r.items[r.tail] = Entry[T]{Value: v, Enqueued: now}
	r.tail = (r.tail + 1) % len(r.items)
	r.len++
	return true
}

func (r *Ring[T]) Pop() (Entry[T], bool) {
	var zero Entry[T]
	if r.len == 0 {
		return zero, false
	}
	entry := r.items[r.head]
	r.items[r.head] = zero
	r.head = (r.head + 1) % len(r.items)
	r.len--
	return entry, true
}

func (r *Ring[T]) DropOldestAndPush(v T, now int64) bool {
	if r.len == 0 {
		return r.Push(v, now)
	}
	if r.len < len(r.items) {
		return r.Push(v, now)
	}
	r.items[r.head] = Entry[T]{Value: v, Enqueued: now}
	r.head = (r.head + 1) % len(r.items)
	r.tail = r.head
	return true
}

func (r *Ring[T]) OldestAge(now int64) time.Duration {
	if r.len == 0 {
		return 0
	}
	return time.Duration(now - r.items[r.head].Enqueued)
}
