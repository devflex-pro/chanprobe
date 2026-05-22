# chanprobe

Observable bounded queues for Go services.

`chanprobe` is a small Go library for instrumenting important async boundaries:
worker pools, event pipelines, webhook delivery, background jobs, rate-limited
integrations, Kafka/NATS consumers, and similar queues.

The goal is not to replace every `chan`. The goal is to make production queues
visible:

- Is the producer blocked?
- Is the consumer slow?
- How long do items wait in the queue?
- Are we dropping work?
- Which queue introduced latency?
- When did backpressure start?

## Installation

```bash
go get github.com/devflex-pro/chanprobe
```

## Basic Example

```go
package main

import (
    "context"
    "fmt"

    "github.com/devflex-pro/chanprobe"
)

func main() {
    ctx := context.Background()
    jobs := chanprobe.New[string]("jobs", 1024)
    defer jobs.Close()

    if err := jobs.Send(ctx, "hello"); err != nil {
        panic(err)
    }

    job, ok := jobs.Recv(ctx)
    if !ok {
        return
    }

    fmt.Println(job)
}
```

## Drop Policies

The default policy is `Block`: `Send` waits for capacity and `TrySend` fails
immediately when the queue is full.

```go
q := chanprobe.New[string](
    "webhook_delivery",
    10_000,
    chanprobe.WithDropPolicy(chanprobe.DropOldest),
)
```

Available policies:

- `Block`: wait for space in `Send`.
- `DropNewest`: reject incoming work when full.
- `DropOldest`: evict the oldest queued item and insert the new one.

## Metrics

Every queue exposes a point-in-time snapshot:

```go
snap := jobs.Snapshot()

fmt.Println("queue", snap.Name)
fmt.Println("depth", snap.Len, "of", snap.Cap)
fmt.Println("sent", snap.SentTotal)
fmt.Println("received", snap.ReceivedTotal)
fmt.Println("dropped", snap.DroppedTotal)
fmt.Println("oldest age", snap.OldestItemAge)
```

Snapshot fields:

- `len`
- `cap`
- `sent_total`
- `received_total`
- `dropped_total`
- `send_blocked_total`
- `recv_blocked_total`
- `send_wait_total`
- `recv_wait_total`
- `item_wait_total`
- `oldest_item_age`
- `closed`

`dropped_total` counts work the queue actually discarded: `DropNewest`
rejections and `DropOldest` evictions. A failed `TrySend` on a full blocking
queue is not counted as dropped work.

## expvar

Queues register in the default registry unless registration is disabled with
`WithRegistry(nil)`.

```go
chanprobe.PublishExpvar("chanprobe", nil)
http.ListenAndServe(":8080", nil)
```

Then inspect:

```bash
curl http://localhost:8080/debug/vars
```

See [examples/http_debug](examples/http_debug/main.go) for a runnable example.

## Performance

`chanprobe` adds observability and context-aware queue operations. It has
overhead compared to native channels. Use it at important async boundaries where
visibility matters, not as a blanket replacement for every channel.

Run local benchmarks with:

```bash
go test -bench=. -benchmem ./...
```

## When Not To Use This Package

- For tiny internal channels where native channel performance matters most.
- For hot loops that do not need queue metrics.
- When you need a drop-in `chan T` replacement.
- When your metrics backend already instruments the queue boundary directly.

## Non-goals

- No `unsafe` in the core package.
- No runtime monkey-patching.
- No magical resizing of Go channels.
- No global goroutine scanning.
- No mandatory Prometheus or OpenTelemetry dependency in the core package.

## License

MIT
