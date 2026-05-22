package chanprobe

import (
	"context"
	"sync"
	"time"

	"github.com/devflex-pro/chanprobe/internal/ring"
)

var clockStart = time.Now()

// Queue is a bounded FIFO queue with observable state and counters.
type Queue[T any] struct {
	name string
	opts Options

	mu              sync.Mutex
	notEmpty        *sync.Cond
	notFull         *sync.Cond
	notEmptyCh      chan struct{}
	notFullCh       chan struct{}
	recvCondWaiters int
	sendCondWaiters int
	closed          bool
	ring            *ring.Ring[T]

	sentTotal        uint64
	receivedTotal    uint64
	droppedTotal     uint64
	sendBlockedTotal uint64
	recvBlockedTotal uint64
	sendWaitTotal    time.Duration
	recvWaitTotal    time.Duration
	itemWaitTotal    time.Duration
}

// New creates a named bounded queue with positive capacity.
// It panics for an empty name or non-positive capacity.
func New[T any](name string, capacity int, opts ...Option) *Queue[T] {
	if name == "" {
		panic("chanprobe: name must be non-empty")
	}
	if capacity <= 0 {
		panic("chanprobe: capacity must be positive")
	}

	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	q := &Queue[T]{
		name: name,
		opts: options,
		ring: ring.New[T](capacity),
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)

	if options.Registry != nil {
		options.Registry.Register(name, q)
	}

	return q
}

// Name returns the queue name.
func (q *Queue[T]) Name() string {
	return q.name
}

// Send inserts an item, waiting when needed under the Block drop policy.
// It returns ErrClosed after Close and returns ctx.Err() if the context is
// canceled while waiting.
func (q *Queue[T]) Send(ctx context.Context, item T) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var waitStart time.Time
	blocked := false

	q.mu.Lock()

	for {
		if q.closed {
			if blocked {
				q.sendWaitTotal += time.Since(waitStart)
			}
			q.mu.Unlock()
			return ErrClosed
		}

		if q.ring.Len() < q.ring.Cap() {
			now := monotonicNow()
			q.ring.Push(item, now)
			q.sentTotal++
			if blocked {
				q.sendWaitTotal += time.Since(waitStart)
			}
			q.signalNotEmptyLocked()
			q.mu.Unlock()
			return nil
		}

		switch q.opts.DropPolicy {
		case DropNewest:
			q.droppedTotal++
			q.mu.Unlock()
			return ErrFull
		case DropOldest:
			now := monotonicNow()
			q.ring.DropOldestAndPush(item, now)
			q.sentTotal++
			q.droppedTotal++
			q.signalNotEmptyLocked()
			q.mu.Unlock()
			return nil
		case Block:
			if err := ctx.Err(); err != nil {
				q.mu.Unlock()
				return err
			}
			if !blocked {
				blocked = true
				waitStart = time.Now()
				q.sendBlockedTotal++
			}
			if ctx.Done() == nil {
				q.sendCondWaiters++
				q.notFull.Wait()
				q.sendCondWaiters--
				continue
			}
			ch := q.notFullChanLocked()
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				q.mu.Lock()
				q.sendWaitTotal += time.Since(waitStart)
				q.mu.Unlock()
				return ctx.Err()
			case <-ch:
				q.mu.Lock()
			}
		default:
			q.mu.Unlock()
			return ErrFull
		}
	}
}

// Recv removes and returns the oldest item. It returns ok=false when the queue
// is closed and drained or when ctx is canceled while waiting.
func (q *Queue[T]) Recv(ctx context.Context) (T, bool) {
	if ctx == nil {
		ctx = context.Background()
	}

	var waitStart time.Time
	blocked := false

	q.mu.Lock()

	for {
		entry, ok := q.ring.Pop()
		if ok {
			now := monotonicNow()
			q.receivedTotal++
			q.itemWaitTotal += time.Duration(now - entry.Enqueued)
			if blocked {
				q.recvWaitTotal += time.Since(waitStart)
			}
			q.signalNotFullLocked()
			q.mu.Unlock()
			return entry.Value, true
		}

		var zero T
		if q.closed {
			if blocked {
				q.recvWaitTotal += time.Since(waitStart)
			}
			q.mu.Unlock()
			return zero, false
		}
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return zero, false
		}
		if !blocked {
			blocked = true
			waitStart = time.Now()
			q.recvBlockedTotal++
		}
		if ctx.Done() == nil {
			q.recvCondWaiters++
			q.notEmpty.Wait()
			q.recvCondWaiters--
			continue
		}
		ch := q.notEmptyChanLocked()
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.recvWaitTotal += time.Since(waitStart)
			q.mu.Unlock()
			return zero, false
		case <-ch:
			q.mu.Lock()
		}
	}
}

// TrySend attempts to insert an item without blocking.
func (q *Queue[T]) TrySend(item T) bool {
	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()
		return false
	}

	if q.ring.Len() < q.ring.Cap() {
		now := monotonicNow()
		q.ring.Push(item, now)
		q.sentTotal++
		q.signalNotEmptyLocked()
		q.mu.Unlock()
		return true
	}

	switch q.opts.DropPolicy {
	case DropOldest:
		now := monotonicNow()
		q.ring.DropOldestAndPush(item, now)
		q.sentTotal++
		q.droppedTotal++
		q.signalNotEmptyLocked()
		q.mu.Unlock()
		return true
	case DropNewest:
		q.droppedTotal++
		q.mu.Unlock()
		return false
	case Block:
		q.mu.Unlock()
		return false
	default:
		q.mu.Unlock()
		return false
	}
}

// TryRecv attempts to remove the oldest item without blocking.
func (q *Queue[T]) TryRecv() (T, bool) {
	q.mu.Lock()

	entry, ok := q.ring.Pop()
	if !ok {
		var zero T
		q.mu.Unlock()
		return zero, false
	}

	q.receivedTotal++
	q.itemWaitTotal += time.Duration(monotonicNow() - entry.Enqueued)
	q.signalNotFullLocked()
	q.mu.Unlock()
	return entry.Value, true
}

// Close marks the queue closed and wakes blocked senders and receivers.
// Already queued items remain receivable.
func (q *Queue[T]) Close() {
	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.closeNotifiersLocked()
	q.mu.Unlock()
}

// Len returns the current queue length.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	n := q.ring.Len()
	q.mu.Unlock()
	return n
}

func (q *Queue[T]) notEmptyChanLocked() chan struct{} {
	if q.notEmptyCh == nil {
		q.notEmptyCh = make(chan struct{})
	}
	return q.notEmptyCh
}

func (q *Queue[T]) notFullChanLocked() chan struct{} {
	if q.notFullCh == nil {
		q.notFullCh = make(chan struct{})
	}
	return q.notFullCh
}

func (q *Queue[T]) signalNotEmptyLocked() {
	if q.recvCondWaiters > 0 {
		q.notEmpty.Signal()
	}
	if q.notEmptyCh == nil {
		return
	}
	close(q.notEmptyCh)
	q.notEmptyCh = make(chan struct{})
}

func (q *Queue[T]) signalNotFullLocked() {
	if q.sendCondWaiters > 0 {
		q.notFull.Signal()
	}
	if q.notFullCh == nil {
		return
	}
	close(q.notFullCh)
	q.notFullCh = make(chan struct{})
}

func (q *Queue[T]) closeNotifiersLocked() {
	if q.notEmptyCh != nil {
		close(q.notEmptyCh)
		q.notEmptyCh = nil
	}
	if q.notFullCh != nil {
		close(q.notFullCh)
		q.notFullCh = nil
	}
	if q.recvCondWaiters > 0 {
		q.notEmpty.Broadcast()
	}
	if q.sendCondWaiters > 0 {
		q.notFull.Broadcast()
	}
}

// Cap returns the queue capacity.
func (q *Queue[T]) Cap() int {
	q.mu.Lock()
	n := q.ring.Cap()
	q.mu.Unlock()
	return n
}

// Snapshot returns a point-in-time copy of queue state and counters.
func (q *Queue[T]) Snapshot() Snapshot {
	q.mu.Lock()

	snap := Snapshot{
		Name:             q.name,
		Len:              q.ring.Len(),
		Cap:              q.ring.Cap(),
		Closed:           q.closed,
		SentTotal:        q.sentTotal,
		ReceivedTotal:    q.receivedTotal,
		DroppedTotal:     q.droppedTotal,
		SendBlockedTotal: q.sendBlockedTotal,
		RecvBlockedTotal: q.recvBlockedTotal,
		SendWaitTotal:    q.sendWaitTotal,
		RecvWaitTotal:    q.recvWaitTotal,
		ItemWaitTotal:    q.itemWaitTotal,
		OldestItemAge:    q.ring.OldestAge(monotonicNow()),
	}
	q.mu.Unlock()
	return snap
}

func monotonicNow() int64 {
	return int64(time.Since(clockStart))
}
