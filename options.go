package chanprobe

// DropPolicy controls what Send and TrySend do when the queue is full.
type DropPolicy int

const (
	// Block makes Send wait for space and makes TrySend fail when the queue is full.
	Block DropPolicy = iota

	// DropNewest rejects the incoming item when the queue is full.
	DropNewest

	// DropOldest evicts the oldest queued item and inserts the incoming item.
	DropOldest
)

// Options configures a Queue.
type Options struct {
	DropPolicy DropPolicy
	Registry   *Registry
}

// Option changes queue construction options.
type Option func(*Options)

// WithDropPolicy sets the queue behavior when it is full.
func WithDropPolicy(policy DropPolicy) Option {
	return func(o *Options) {
		o.DropPolicy = policy
	}
}

// WithRegistry sets the registry used for automatic queue registration.
// Passing nil disables registration.
func WithRegistry(reg *Registry) Option {
	return func(o *Options) {
		o.Registry = reg
	}
}

func defaultOptions() Options {
	return Options{
		DropPolicy: Block,
		Registry:   DefaultRegistry(),
	}
}
