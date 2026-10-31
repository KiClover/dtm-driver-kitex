package testkit

import (
	"fmt"
	"net"
	"sync"

	"github.com/cloudwego/kitex/pkg/registry"
)

// Registry is a fake kitex registry.Registry that records Register and
// Deregister calls and can script their outcomes.
type Registry struct {
	mu sync.Mutex

	registers   []registry.Info
	deregisters []registry.Info

	registerErr   error
	deregisterErr error
}

var _ registry.Registry = (*Registry)(nil)

// NewRegistry creates an empty fake registry.
func NewRegistry() *Registry { return &Registry{} }

// SetRegisterError scripts the next Register outcomes with err (nil restores
// success).
func (r *Registry) SetRegisterError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registerErr = err
}

// SetDeregisterError scripts the Deregister outcome.
func (r *Registry) SetDeregisterError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deregisterErr = err
}

// Register records info and returns the scripted error.
func (r *Registry) Register(info *registry.Info) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if info == nil {
		return fmt.Errorf("testkit.Registry: nil info")
	}
	r.registers = append(r.registers, *info)
	return r.registerErr
}

// Deregister records info and returns the scripted error.
func (r *Registry) Deregister(info *registry.Info) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if info == nil {
		return fmt.Errorf("testkit.Registry: nil info")
	}
	r.deregisters = append(r.deregisters, *info)
	return r.deregisterErr
}

// Registers returns the recorded Register infos in order.
func (r *Registry) Registers() []registry.Info {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]registry.Info(nil), r.registers...)
}

// Deregisters returns the recorded Deregister infos in order.
func (r *Registry) Deregisters() []registry.Info {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]registry.Info(nil), r.deregisters...)
}

// LastRegister returns the most recent Register info.
func (r *Registry) LastRegister() (registry.Info, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.registers) == 0 {
		return registry.Info{}, false
	}
	return r.registers[len(r.registers)-1], true
}

// TCPAddr builds a *net.TCPAddr for assertions.
func TCPAddr(host string, port int) *net.TCPAddr {
	return &net.TCPAddr{IP: net.ParseIP(host), Port: port}
}
