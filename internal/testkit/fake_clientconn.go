package testkit

import (
	"fmt"
	"sync"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// ClientConn is a recording grpc-go resolver.ClientConn.
type ClientConn struct {
	mu sync.Mutex

	states []resolver.State
	errors []error

	serviceConfigs []string

	// updateStateErr, when set, makes every UpdateState call return it.
	updateStateErr error
}

var _ resolver.ClientConn = (*ClientConn)(nil)

// NewClientConn creates an empty recording ClientConn.
func NewClientConn() *ClientConn { return &ClientConn{} }

// SetUpdateStateError makes subsequent UpdateState calls fail.
func (c *ClientConn) SetUpdateStateError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateStateErr = err
}

// UpdateState records the state and returns the scripted error.
func (c *ClientConn) UpdateState(state resolver.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states = append(c.states, state)
	return c.updateStateErr
}

// ReportError records the reported error.
func (c *ClientConn) ReportError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, err)
}

// NewAddress records a deprecated-style address push as a state.
func (c *ClientConn) NewAddress(addresses []resolver.Address) {
	_ = c.UpdateState(resolver.State{Addresses: addresses})
}

// NewServiceConfig records a deprecated-style service config push.
func (c *ClientConn) NewServiceConfig(serviceConfig string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serviceConfigs = append(c.serviceConfigs, serviceConfig)
}

// ParseServiceConfig satisfies the interface; the fake does not parse configs.
func (c *ClientConn) ParseServiceConfig(serviceConfig string) *serviceconfig.ParseResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serviceConfigs = append(c.serviceConfigs, serviceConfig)
	return &serviceconfig.ParseResult{Err: nil}
}

// States returns a copy of all states passed to UpdateState.
func (c *ClientConn) States() []resolver.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]resolver.State(nil), c.states...)
}

// LastState returns the most recent state and whether any state was seen.
func (c *ClientConn) LastState() (resolver.State, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.states) == 0 {
		return resolver.State{}, false
	}
	return c.states[len(c.states)-1], true
}

// StateCount returns how many states were delivered.
func (c *ClientConn) StateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.states)
}

// Errors returns a copy of all errors passed to ReportError.
func (c *ClientConn) Errors() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errors...)
}

// ErrorCount returns how many errors were reported.
func (c *ClientConn) ErrorCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errors)
}

// Addresses returns the addresses of the last state, for assertions.
func (c *ClientConn) Addresses() []resolver.Address {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.states) == 0 {
		return nil
	}
	return append([]resolver.Address(nil), c.states[len(c.states)-1].Addresses...)
}

// String formats the recorder for failure messages.
func (c *ClientConn) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("states=%d errors=%d", len(c.states), len(c.errors))
}
