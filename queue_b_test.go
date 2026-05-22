package chanprobe

import (
	"context"
	"testing"
)

func BenchmarkChannelSameGoroutine(b *testing.B) {
	ch := make(chan int, 1)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ch <- i
		<-ch
	}
}

func BenchmarkQueueSameGoroutine(b *testing.B) {
	q := New[int]("bench_queue_same_goroutine", 1, WithRegistry(nil))
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := q.Send(ctx, i); err != nil {
			b.Fatal(err)
		}
		if _, ok := q.Recv(ctx); !ok {
			b.Fatal("recv failed")
		}
	}
}

func BenchmarkChannelProducerConsumer(b *testing.B) {
	ch := make(chan int, 1024)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for range ch {
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	b.StopTimer()

	close(ch)
	<-done
}

func BenchmarkQueueProducerConsumer(b *testing.B) {
	q := New[int]("bench_queue_producer_consumer", 1024, WithRegistry(nil))
	ctx := context.Background()
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			if _, ok := q.Recv(ctx); !ok {
				return
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Send(ctx, i); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	q.Close()
	<-done
}

func BenchmarkQueueTrySendTryRecv(b *testing.B) {
	q := New[int]("bench_queue_try_send_try_recv", 1, WithRegistry(nil))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !q.TrySend(i) {
			b.Fatal("try send failed")
		}
		if _, ok := q.TryRecv(); !ok {
			b.Fatal("try recv failed")
		}
	}
}

func BenchmarkQueueSnapshot(b *testing.B) {
	q := New[int]("bench_queue_snapshot", 1024, WithRegistry(nil))
	for i := 0; i < q.Cap(); i++ {
		if !q.TrySend(i) {
			b.Fatal("try send failed")
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Snapshot()
	}
}
