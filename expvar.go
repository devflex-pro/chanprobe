package chanprobe

import "expvar"

// PublishExpvar publishes registry snapshots under name through expvar.
// If name is empty, "chanprobe" is used. If reg is nil, DefaultRegistry is used.
func PublishExpvar(name string, reg *Registry) {
	if name == "" {
		name = "chanprobe"
	}
	if reg == nil {
		reg = DefaultRegistry()
	}

	expvar.Publish(name, expvar.Func(func() any {
		return reg.Snapshots()
	}))
}
