# Public API

```go
package chanprobe

var (
    ErrClosed error
    ErrFull   error
)

type DropPolicy int

const (
    Block DropPolicy = iota
    DropNewest
    DropOldest
)

type Option func(*Options)

type Options struct {
    DropPolicy DropPolicy
    Registry   *Registry
}

func WithDropPolicy(policy DropPolicy) Option
func WithRegistry(reg *Registry) Option

type Queue[T any] struct{}

func New[T any](name string, capacity int, opts ...Option) *Queue[T]

func (q *Queue[T]) Name() string
func (q *Queue[T]) Send(ctx context.Context, item T) error
func (q *Queue[T]) Recv(ctx context.Context) (T, bool)
func (q *Queue[T]) TrySend(item T) bool
func (q *Queue[T]) TryRecv() (T, bool)
func (q *Queue[T]) Close()
func (q *Queue[T]) Len() int
func (q *Queue[T]) Cap() int
func (q *Queue[T]) Snapshot() Snapshot

type Snapshot struct {
    Name             string        `json:"name"`
    Len              int           `json:"len"`
    Cap              int           `json:"cap"`
    Closed           bool          `json:"closed"`
    SentTotal        uint64        `json:"sent_total"`
    ReceivedTotal    uint64        `json:"received_total"`
    DroppedTotal     uint64        `json:"dropped_total"`
    SendBlockedTotal uint64        `json:"send_blocked_total"`
    RecvBlockedTotal uint64        `json:"recv_blocked_total"`
    SendWaitTotal    time.Duration `json:"send_wait_total"`
    RecvWaitTotal    time.Duration `json:"recv_wait_total"`
    ItemWaitTotal    time.Duration `json:"item_wait_total"`
    OldestItemAge    time.Duration `json:"oldest_item_age"`
}

type Snapshoter interface {
    Snapshot() Snapshot
}

type Registry struct{}

func NewRegistry() *Registry
func DefaultRegistry() *Registry
func (r *Registry) Register(name string, snapper Snapshoter)
func (r *Registry) Unregister(name string)
func (r *Registry) Snapshots() []Snapshot

func PublishExpvar(name string, reg *Registry)
```

## Notes

- `New` panics for an empty name or non-positive capacity.
- `Send` returns `ErrClosed` after `Close`.
- `Send` returns `ctx.Err()` when a blocking send is canceled.
- `Recv` returns `ok=false` when the queue is closed and drained, or when a
  blocking receive is canceled.
- `PublishExpvar` uses `DefaultRegistry()` when the registry argument is nil.
