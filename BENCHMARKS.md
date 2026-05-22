# Benchmark plan

Benchmarks should be honest and not overclaim.

Run:

```bash
go test -bench=. -benchmem ./...
```

For more stable local numbers, run multiple samples:

```bash
go test -run='^$' -bench=. -benchmem -count=5 ./...
```

## Benchmark cases

1. Plain channel send/recv same goroutine.
2. `chanprobe.Queue` Send/Recv same goroutine.
3. Plain channel producer/consumer.
4. `chanprobe.Queue` producer/consumer.
5. TrySend/TryRecv.
6. Snapshot under load.

## Interpreting results

Compare related pairs:

- `BenchmarkChannelSameGoroutine` vs `BenchmarkQueueSameGoroutine`
- `BenchmarkChannelProducerConsumer` vs `BenchmarkQueueProducerConsumer`

Native channels are the baseline. `chanprobe.Queue` is expected to cost more
because it tracks counters, wait durations, item age, close state, and supports
context-aware blocking operations.

`BenchmarkQueueTrySendTryRecv` measures the non-blocking fast path.

`BenchmarkQueueSnapshot` measures the cost of reading observability data from a
populated queue.

Example local result on an AMD Ryzen 3 4300U:

```text
BenchmarkChannelSameGoroutine-4       30.80 ns/op    0 B/op  0 allocs/op
BenchmarkQueueSameGoroutine-4       2293 ns/op       0 B/op  0 allocs/op
BenchmarkChannelProducerConsumer-4    51.11 ns/op    0 B/op  0 allocs/op
BenchmarkQueueProducerConsumer-4    2389 ns/op       0 B/op  0 allocs/op
BenchmarkQueueTrySendTryRecv-4      2262 ns/op       0 B/op  0 allocs/op
BenchmarkQueueSnapshot-4            1164 ns/op       0 B/op  0 allocs/op
```

Treat these as local reference numbers, not a universal performance claim.

## README benchmark wording

Do not claim `chanprobe` is faster than channels.

Expected positioning:

> `chanprobe` adds observability and context-aware queue operations. It has overhead compared to native channels. Use it at important async boundaries where visibility matters.
