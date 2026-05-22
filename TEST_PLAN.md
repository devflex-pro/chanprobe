# Test plan

## Unit tests

Use deterministic synchronization with channels, contexts, and wait groups.

Avoid long sleeps.

### Ring

- Empty ring returns false.
- Push/pop maintains FIFO.
- Full ring rejects push.
- DropOldest removes oldest and inserts new item.
- Len and Cap are correct.

### Queue FIFO

Send N items, receive N items, assert order.

### Blocking Send

Create capacity 1 queue. Fill it. Start goroutine calling `Send`. Verify it blocks. Receive one item. Verify sender completes.

### Blocking Recv

Start goroutine calling `Recv`. Verify it blocks. Send one item. Verify receiver completes.

### Context cancellation

- `Send` on full queue exits with `context.Canceled`.
- `Recv` on empty queue exits with `ok=false`.

### Close

- Send after close returns `ErrClosed`.
- TrySend after close returns false.
- Recv drains existing items.
- Recv after drain returns false.
- Close wakes blocked senders and receivers.
- Close can be called multiple times.

### DropNewest

- Fill queue.
- TrySend returns false.
- Send returns `ErrFull`.
- Existing items remain.

### DropOldest

- Fill queue.
- Send new item.
- Oldest item is gone.
- New item is present.
- DroppedTotal increments.

### Snapshot

- Counters update correctly.
- OldestItemAge is positive when queue has an item.
- OldestItemAge is zero when queue is empty.

## Race tests

Run:

```bash
go test -race ./...
```

Add a stress test with multiple producers and consumers.
