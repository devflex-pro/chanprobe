// Package chanprobe provides observable bounded queues for important async
// boundaries in Go services.
//
// A Queue is not a drop-in replacement for a channel. It adds a small explicit
// API for bounded FIFO delivery, context-aware blocking sends and receives,
// configurable drop policies, point-in-time snapshots, and optional expvar
// export.
package chanprobe
