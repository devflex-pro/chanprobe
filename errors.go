package chanprobe

import "errors"

var (
	// ErrClosed is returned by Send when the queue has been closed.
	ErrClosed = errors.New("chanprobe: queue closed")

	// ErrFull is returned by Send when a full queue rejects an item.
	ErrFull = errors.New("chanprobe: queue full")
)
