package chanprobe

import (
	"sort"
	"sync"
)

// Registry stores named queues and returns snapshots for all registered queues.
type Registry struct {
	mu     sync.RWMutex
	queues map[string]Snapshoter
}

var defaultRegistry = NewRegistry()

// NewRegistry returns an empty queue registry.
func NewRegistry() *Registry {
	return &Registry{
		queues: make(map[string]Snapshoter),
	}
}

// DefaultRegistry returns the process-wide registry used by New by default.
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// Register stores a named snapshot provider. Invalid inputs are ignored.
func (r *Registry) Register(name string, q Snapshoter) {
	if r == nil || name == "" || q == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queues[name] = q
}

// Unregister removes a name from the registry.
func (r *Registry) Unregister(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.queues, name)
}

// Snapshots returns snapshots for registered queues sorted by name.
func (r *Registry) Snapshots() []Snapshot {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.queues))
	for name := range r.queues {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]Snapshot, 0, len(names))
	for _, name := range names {
		out = append(out, r.queues[name].Snapshot())
	}
	return out
}
