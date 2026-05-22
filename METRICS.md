# Metrics design

## Core principle

The core package exposes snapshots. Exporters convert snapshots to external systems.

This avoids forcing dependencies on users.

## Snapshot fields

### Len and Cap

Current queue occupancy.

Useful to detect saturation but not enough alone.

### SentTotal and ReceivedTotal

Basic throughput counters.

### DroppedTotal

Critical for lossy queues.

Increment when:

- `DropNewest` rejects incoming work.
- `DropOldest` evicts old work.

### SendBlockedTotal

Increment when a `Send` call found a full queue and had to wait under `Block`.

### RecvBlockedTotal

Increment when a `Recv` call found an empty queue and had to wait.

### SendWaitTotal and RecvWaitTotal

Total accumulated waiting time.

Average wait can be derived as:

```text
avg_send_wait = SendWaitTotal / SendBlockedTotal
avg_recv_wait = RecvWaitTotal / RecvBlockedTotal
```

### ItemWaitTotal

Total time items spent inside the queue before being received.

Average queue age can be derived as:

```text
avg_item_wait = ItemWaitTotal / ReceivedTotal
```

### OldestItemAge

Age of the oldest item currently in the queue.

This is one of the most useful incident metrics.

## v1 deliberately avoids histograms

Histograms require either:

- a dependency on a metrics backend,
- a custom approximate histogram implementation,
- or more API surface.

For v1, snapshots are enough. Prometheus/OpenTelemetry adapters can add histograms later.
