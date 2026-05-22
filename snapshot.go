package chanprobe

import "time"

// Snapshot is a point-in-time view of a queue's state and counters.
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

// Snapshoter is implemented by values that can report a queue snapshot.
type Snapshoter interface {
	Snapshot() Snapshot
}
