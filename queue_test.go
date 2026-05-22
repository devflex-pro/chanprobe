package chanprobe

import (
	"context"
	"errors"
	"expvar"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTrySendTryRecvFIFO(t *testing.T) {
	q := New[int]("test_fifo", 2, WithRegistry(nil))

	if !q.TrySend(1) || !q.TrySend(2) {
		t.Fatal("expected sends to succeed")
	}

	v, ok := q.TryRecv()
	if !ok || v != 1 {
		t.Fatalf("got %v %v, want 1 true", v, ok)
	}

	v, ok = q.TryRecv()
	if !ok || v != 2 {
		t.Fatalf("got %v %v, want 2 true", v, ok)
	}
}

func TestCloseDrains(t *testing.T) {
	q := New[int]("test_close", 2, WithRegistry(nil))
	q.TrySend(1)
	q.Close()

	if q.TrySend(2) {
		t.Fatal("send after close should fail")
	}

	v, ok := q.TryRecv()
	if !ok || v != 1 {
		t.Fatalf("got %v %v, want 1 true", v, ok)
	}

	_, ok = q.TryRecv()
	if ok {
		t.Fatal("expected drained queue")
	}
}

func TestDropOldest(t *testing.T) {
	q := New[int]("test_drop_oldest", 2, WithDropPolicy(DropOldest), WithRegistry(nil))

	q.TrySend(1)
	q.TrySend(2)
	if !q.TrySend(3) {
		t.Fatal("drop oldest should accept new item")
	}

	v, _ := q.TryRecv()
	if v != 2 {
		t.Fatalf("got %d, want 2", v)
	}

	v, _ = q.TryRecv()
	if v != 3 {
		t.Fatalf("got %d, want 3", v)
	}
}

func TestTrySendFullBlockDoesNotDrop(t *testing.T) {
	q := New[int]("test_try_send_full_block", 1, WithRegistry(nil))

	if !q.TrySend(1) {
		t.Fatal("expected initial send to succeed")
	}
	if q.TrySend(2) {
		t.Fatal("expected full block-policy queue to reject TrySend")
	}

	snap := q.Snapshot()
	if snap.DroppedTotal != 0 {
		t.Fatalf("DroppedTotal = %d, want 0", snap.DroppedTotal)
	}
}

func TestDropNewest(t *testing.T) {
	q := New[int]("test_drop_newest", 1, WithDropPolicy(DropNewest), WithRegistry(nil))

	if !q.TrySend(1) {
		t.Fatal("expected initial send to succeed")
	}
	if q.TrySend(2) {
		t.Fatal("expected TrySend to reject newest item")
	}
	if err := q.Send(context.Background(), 3); !errors.Is(err, ErrFull) {
		t.Fatalf("Send error = %v, want ErrFull", err)
	}

	v, ok := q.TryRecv()
	if !ok || v != 1 {
		t.Fatalf("got %v %v, want 1 true", v, ok)
	}

	snap := q.Snapshot()
	if snap.DroppedTotal != 2 {
		t.Fatalf("DroppedTotal = %d, want 2", snap.DroppedTotal)
	}
}

func TestDropPoliciesTable(t *testing.T) {
	tests := []struct {
		name        string
		policy      DropPolicy
		sendErr     error
		trySendOK   bool
		wantValues  []int
		wantDropped uint64
	}{
		{
			name:        "block",
			policy:      Block,
			trySendOK:   false,
			wantValues:  []int{1, 2},
			wantDropped: 0,
		},
		{
			name:        "drop_newest",
			policy:      DropNewest,
			sendErr:     ErrFull,
			trySendOK:   false,
			wantValues:  []int{1, 2},
			wantDropped: 2,
		},
		{
			name:        "drop_oldest",
			policy:      DropOldest,
			trySendOK:   true,
			wantValues:  []int{3, 4},
			wantDropped: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := New[int]("test_drop_policy_"+tt.name, 2, WithDropPolicy(tt.policy), WithRegistry(nil))

			if err := q.Send(context.Background(), 1); err != nil {
				t.Fatalf("send 1: %v", err)
			}
			if err := q.Send(context.Background(), 2); err != nil {
				t.Fatalf("send 2: %v", err)
			}

			if tt.policy != Block {
				err := q.Send(context.Background(), 3)
				if !errors.Is(err, tt.sendErr) {
					t.Fatalf("send 3 error = %v, want %v", err, tt.sendErr)
				}
			}

			if ok := q.TrySend(4); ok != tt.trySendOK {
				t.Fatalf("TrySend = %v, want %v", ok, tt.trySendOK)
			}

			for _, want := range tt.wantValues {
				got, ok := q.TryRecv()
				if !ok || got != want {
					t.Fatalf("TryRecv = %v %v, want %v true", got, ok, want)
				}
			}
			if _, ok := q.TryRecv(); ok {
				t.Fatal("queue should be empty")
			}

			if got := q.Snapshot().DroppedTotal; got != tt.wantDropped {
				t.Fatalf("DroppedTotal = %d, want %d", got, tt.wantDropped)
			}
		})
	}
}

func TestSendRecvFIFO(t *testing.T) {
	q := New[int]("test_send_recv_fifo", 2, WithRegistry(nil))

	if err := q.Send(context.Background(), 1); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if err := q.Send(context.Background(), 2); err != nil {
		t.Fatalf("send 2: %v", err)
	}

	v, ok := q.Recv(context.Background())
	if !ok || v != 1 {
		t.Fatalf("got %v %v, want 1 true", v, ok)
	}
	v, ok = q.Recv(context.Background())
	if !ok || v != 2 {
		t.Fatalf("got %v %v, want 2 true", v, ok)
	}
}

func TestBlockingSendWaitsForSpace(t *testing.T) {
	q := New[int]("test_blocking_send", 1, WithRegistry(nil))
	if err := q.Send(context.Background(), 1); err != nil {
		t.Fatalf("initial send: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- q.Send(context.Background(), 2)
	}()
	<-started

	assertStillBlocked(t, done)

	v, ok := q.Recv(context.Background())
	if !ok || v != 1 {
		t.Fatalf("got %v %v, want 1 true", v, ok)
	}

	if err := receiveSoon(t, done); err != nil {
		t.Fatalf("blocked send returned error: %v", err)
	}

	v, ok = q.Recv(context.Background())
	if !ok || v != 2 {
		t.Fatalf("got %v %v, want 2 true", v, ok)
	}
}

func TestBlockingRecvWaitsForItem(t *testing.T) {
	q := New[int]("test_blocking_recv", 1, WithRegistry(nil))

	started := make(chan struct{})
	done := make(chan recvResult[int], 1)
	go func() {
		close(started)
		v, ok := q.Recv(context.Background())
		done <- recvResult[int]{value: v, ok: ok}
	}()
	<-started

	assertStillBlocked(t, done)

	if err := q.Send(context.Background(), 7); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := receiveSoon(t, done)
	if !got.ok || got.value != 7 {
		t.Fatalf("got %v %v, want 7 true", got.value, got.ok)
	}
}

func TestSendContextCancellation(t *testing.T) {
	q := New[int]("test_send_context_cancel", 1, WithRegistry(nil))
	if err := q.Send(context.Background(), 1); err != nil {
		t.Fatalf("initial send: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- q.Send(ctx, 2)
	}()

	assertStillBlocked(t, done)
	cancel()

	if err := receiveSoon(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestRecvContextCancellation(t *testing.T) {
	q := New[int]("test_recv_context_cancel", 1, WithRegistry(nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan recvResult[int], 1)
	go func() {
		v, ok := q.Recv(ctx)
		done <- recvResult[int]{value: v, ok: ok}
	}()

	assertStillBlocked(t, done)
	cancel()

	got := receiveSoon(t, done)
	if got.ok {
		t.Fatalf("got ok=true, want false")
	}
}

func TestCloseWakesBlockedSendAndRecv(t *testing.T) {
	t.Run("send", func(t *testing.T) {
		q := New[int]("test_close_wakes_send", 1, WithRegistry(nil))
		if err := q.Send(context.Background(), 1); err != nil {
			t.Fatalf("initial send: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- q.Send(context.Background(), 2)
		}()
		assertStillBlocked(t, done)

		q.Close()

		if err := receiveSoon(t, done); !errors.Is(err, ErrClosed) {
			t.Fatalf("got %v, want ErrClosed", err)
		}
	})

	t.Run("recv", func(t *testing.T) {
		q := New[int]("test_close_wakes_recv", 1, WithRegistry(nil))

		done := make(chan recvResult[int], 1)
		go func() {
			v, ok := q.Recv(context.Background())
			done <- recvResult[int]{value: v, ok: ok}
		}()
		assertStillBlocked(t, done)

		q.Close()

		got := receiveSoon(t, done)
		if got.ok {
			t.Fatalf("got ok=true, want false")
		}
	})
}

func TestSnapshotCounters(t *testing.T) {
	q := New[int]("test_snapshot_counters", 2, WithRegistry(nil))

	if err := q.Send(context.Background(), 1); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if err := q.Send(context.Background(), 2); err != nil {
		t.Fatalf("send 2: %v", err)
	}

	snap := q.Snapshot()
	if snap.Name != "test_snapshot_counters" {
		t.Fatalf("Name = %q, want test_snapshot_counters", snap.Name)
	}
	if snap.Len != 2 || snap.Cap != 2 {
		t.Fatalf("Len/Cap = %d/%d, want 2/2", snap.Len, snap.Cap)
	}
	if snap.SentTotal != 2 || snap.ReceivedTotal != 0 || snap.DroppedTotal != 0 {
		t.Fatalf("counters = sent %d received %d dropped %d, want 2/0/0", snap.SentTotal, snap.ReceivedTotal, snap.DroppedTotal)
	}
	if snap.OldestItemAge <= 0 {
		t.Fatalf("OldestItemAge = %v, want positive", snap.OldestItemAge)
	}

	v, ok := q.Recv(context.Background())
	if !ok || v != 1 {
		t.Fatalf("got %v %v, want 1 true", v, ok)
	}
	v, ok = q.Recv(context.Background())
	if !ok || v != 2 {
		t.Fatalf("got %v %v, want 2 true", v, ok)
	}

	snap = q.Snapshot()
	if snap.Len != 0 {
		t.Fatalf("Len = %d, want 0", snap.Len)
	}
	if snap.ReceivedTotal != 2 {
		t.Fatalf("ReceivedTotal = %d, want 2", snap.ReceivedTotal)
	}
	if snap.ItemWaitTotal <= 0 {
		t.Fatalf("ItemWaitTotal = %v, want positive", snap.ItemWaitTotal)
	}
	if snap.OldestItemAge != 0 {
		t.Fatalf("OldestItemAge = %v, want 0", snap.OldestItemAge)
	}
}

func TestSnapshotBlockedCounters(t *testing.T) {
	t.Run("send", func(t *testing.T) {
		q := New[int]("test_snapshot_blocked_send", 1, WithRegistry(nil))
		if err := q.Send(context.Background(), 1); err != nil {
			t.Fatalf("initial send: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- q.Send(context.Background(), 2)
		}()
		assertStillBlocked(t, done)

		if snap := q.Snapshot(); snap.SendBlockedTotal != 1 {
			t.Fatalf("SendBlockedTotal = %d, want 1", snap.SendBlockedTotal)
		}

		_, _ = q.Recv(context.Background())
		if err := receiveSoon(t, done); err != nil {
			t.Fatalf("blocked send returned error: %v", err)
		}

		snap := q.Snapshot()
		if snap.SendWaitTotal <= 0 {
			t.Fatalf("SendWaitTotal = %v, want positive", snap.SendWaitTotal)
		}
	})

	t.Run("recv", func(t *testing.T) {
		q := New[int]("test_snapshot_blocked_recv", 1, WithRegistry(nil))

		done := make(chan recvResult[int], 1)
		go func() {
			v, ok := q.Recv(context.Background())
			done <- recvResult[int]{value: v, ok: ok}
		}()
		assertStillBlocked(t, done)

		if snap := q.Snapshot(); snap.RecvBlockedTotal != 1 {
			t.Fatalf("RecvBlockedTotal = %d, want 1", snap.RecvBlockedTotal)
		}

		if err := q.Send(context.Background(), 1); err != nil {
			t.Fatalf("send: %v", err)
		}
		got := receiveSoon(t, done)
		if !got.ok || got.value != 1 {
			t.Fatalf("got %v %v, want 1 true", got.value, got.ok)
		}

		snap := q.Snapshot()
		if snap.RecvWaitTotal <= 0 {
			t.Fatalf("RecvWaitTotal = %v, want positive", snap.RecvWaitTotal)
		}
	})
}

func TestRegistrySnapshots(t *testing.T) {
	reg := NewRegistry()
	alpha := New[int]("alpha", 1, WithRegistry(reg))
	beta := New[int]("beta", 1, WithRegistry(reg))
	alpha.TrySend(1)
	beta.TrySend(2)

	snaps := reg.Snapshots()
	if len(snaps) != 2 {
		t.Fatalf("len snapshots = %d, want 2", len(snaps))
	}
	if snaps[0].Name != "alpha" || snaps[1].Name != "beta" {
		t.Fatalf("snapshot order = %q, %q; want alpha, beta", snaps[0].Name, snaps[1].Name)
	}

	reg.Unregister("alpha")
	snaps = reg.Snapshots()
	if len(snaps) != 1 || snaps[0].Name != "beta" {
		t.Fatalf("snapshots after unregister = %#v, want only beta", snaps)
	}
}

func TestNilRegistryIsNoop(t *testing.T) {
	var reg *Registry

	reg.Register("name", nil)
	reg.Unregister("name")
	if snaps := reg.Snapshots(); snaps != nil {
		t.Fatalf("nil registry snapshots = %#v, want nil", snaps)
	}
}

func TestPublishExpvar(t *testing.T) {
	reg := NewRegistry()
	q := New[int]("expvar_queue", 1, WithRegistry(reg))
	q.TrySend(1)

	name := "chanprobe_test_expvar"
	if expvar.Get(name) != nil {
		t.Fatalf("expvar %q already registered", name)
	}

	PublishExpvar(name, reg)

	v := expvar.Get(name)
	if v == nil {
		t.Fatalf("expvar %q was not registered", name)
	}
	if got := v.String(); got == "" || got == "null" {
		t.Fatalf("expvar string = %q, want non-empty JSON", got)
	}
}

func TestStressMultipleProducersConsumers(t *testing.T) {
	const (
		producers        = 4
		consumers        = 4
		itemsPerProducer = 250
	)

	q := New[int]("test_stress", 32, WithRegistry(nil))
	var produced atomic.Int64
	var consumed atomic.Int64

	var consumerWG sync.WaitGroup
	consumerWG.Add(consumers)
	for range consumers {
		go func() {
			defer consumerWG.Done()
			for {
				_, ok := q.Recv(context.Background())
				if !ok {
					return
				}
				consumed.Add(1)
			}
		}()
	}

	var producerWG sync.WaitGroup
	producerWG.Add(producers)
	for producer := range producers {
		go func() {
			defer producerWG.Done()
			base := producer * itemsPerProducer
			for i := range itemsPerProducer {
				if err := q.Send(context.Background(), base+i); err != nil {
					t.Errorf("send: %v", err)
					return
				}
				produced.Add(1)
			}
		}()
	}

	producerWG.Wait()
	q.Close()
	consumerWG.Wait()

	want := int64(producers * itemsPerProducer)
	if produced.Load() != want {
		t.Fatalf("produced = %d, want %d", produced.Load(), want)
	}
	if consumed.Load() != want {
		t.Fatalf("consumed = %d, want %d", consumed.Load(), want)
	}

	snap := q.Snapshot()
	if snap.SentTotal != uint64(want) || snap.ReceivedTotal != uint64(want) {
		t.Fatalf("snapshot sent/received = %d/%d, want %d/%d", snap.SentTotal, snap.ReceivedTotal, want, want)
	}
	if snap.Len != 0 {
		t.Fatalf("Len = %d, want 0", snap.Len)
	}
}

type recvResult[T any] struct {
	value T
	ok    bool
}

func assertStillBlocked[T any](t *testing.T, ch <-chan T) {
	t.Helper()

	select {
	case got := <-ch:
		t.Fatalf("operation completed early with %#v", got)
	case <-time.After(10 * time.Millisecond):
	}
}

func receiveSoon[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for operation")
		var zero T
		return zero
	}
}
