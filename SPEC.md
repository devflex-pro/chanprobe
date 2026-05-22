# chanprobe specification

## Concept

`chanprobe.Queue[T]` is a bounded FIFO queue with instrumentation.

It is intentionally not a drop-in replacement for `chan T`; it is an explicit queue abstraction for important async boundaries.

## Constructor

```go
func New[T any](name string, capacity int, opts ...Option) *Queue[T]
```

Rules:

- `name` must be non-empty.
- `capacity` must be greater than zero.
- Invalid options should be handled predictably. Prefer panic for programmer errors in constructor only.

## Methods

```go
func (q *Queue[T]) Send(ctx context.Context, item T) error
func (q *Queue[T]) Recv(ctx context.Context) (T, bool)
func (q *Queue[T]) TrySend(item T) bool
func (q *Queue[T]) TryRecv() (T, bool)
func (q *Queue[T]) Close()
func (q *Queue[T]) Len() int
func (q *Queue[T]) Cap() int
func (q *Queue[T]) Snapshot() Snapshot
```

### Send

`Send` inserts an item.

Behavior:

- If queue has space: insert and return nil.
- If queue is full and policy is `Block`: wait until space, context cancellation, or close.
- If queue is full and policy is `DropNewest`: drop incoming item and return `ErrFull`.
- If queue is full and policy is `DropOldest`: remove oldest item, insert new item, return nil.
- If queue is closed: return `ErrClosed`.
- If context is canceled while waiting: return `ctx.Err()`.

### Recv

`Recv` removes oldest item.

Behavior:

- If an item exists: return `(item, true)`.
- If queue is empty but open: wait until item, context cancellation, or close.
- If queue is empty and closed: return zero value and `false`.
- If context is canceled while waiting: return zero value and `false`.

Note: `Recv` does not return an error to stay close to channel receive semantics.

### TrySend

Non-blocking send.

Behavior:

- Returns true if item was inserted.
- Returns false if closed or full under `Block`/`DropNewest`.
- Under `DropOldest`, full queue should drop oldest, insert new item, and return true.

### TryRecv

Non-blocking receive.

Behavior:

- Returns `(item, true)` if item exists.
- Returns zero value and `false` if empty or closed-empty.

### Close

Behavior:

- Idempotent.
- Wakes all waiters.
- After close, `Send` and `TrySend` fail.
- Existing queued items remain receivable.
- Once drained, `Recv` returns `false`.

## Drop policies

```go
type DropPolicy int

const (
    Block DropPolicy = iota
    DropNewest
    DropOldest
)
```

## Observability

Each queue should maintain enough data to produce a snapshot:

```go
type Snapshot struct {
    Name              string
    Len               int
    Cap               int
    Closed            bool
    SentTotal         uint64
    ReceivedTotal     uint64
    DroppedTotal      uint64
    SendBlockedTotal  uint64
    RecvBlockedTotal  uint64
    SendWaitTotal     time.Duration
    RecvWaitTotal     time.Duration
    ItemWaitTotal     time.Duration
    OldestItemAge     time.Duration
}
```

Histograms are optional for v1. Prefer counters and total durations first.
