package dtmkitex

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Config is the driver-level configuration accepted by Configure. It carries
// only a credential file path and an inline-credentials switch; secrets never
// appear here.
type Config struct {
	// CredentialFile is the path to a YAML file with per-backend credential
	// profiles. Empty means no profiles are available.
	CredentialFile string `json:"credential_file"`

	// AllowInlineCredentials permits credentials embedded in URIs. Default
	// false: inline secrets are rejected.
	AllowInlineCredentials bool `json:"allow_inline_credentials"`
}

// ErrAlreadyStarted is returned when Configure is called after the driver has
// been used at runtime (parse, register, or resolver build).
var ErrAlreadyStarted = fmt.Errorf("dtm-driver-kitex: configuration already frozen by runtime use")

// configState owns the frozen configuration snapshot shared by the package
// helpers and the default driver receiver.
type configState struct {
	mu      sync.RWMutex
	cfg     Config
	frozen  bool
	started bool
}

var defaultConfig = &configState{}

// freeze marks configuration as immutable; called before first runtime use.
func (s *configState) freeze() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.frozen {
		s.frozen = true
	}
}

// start marks the runtime as started; later Configure calls fail.
func (s *configState) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = true
}

// apply stores cfg unless the runtime already started.
func (s *configState) apply(cfg Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return ErrAlreadyStarted
	}
	s.cfg = cfg
	return nil
}

// snapshot returns the current configuration.
func (s *configState) snapshot() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// Configure applies driver configuration on the default driver instance. It
// must be called before the first ParseServerMethod, RegisterService, or
// resolver Build; later calls return ErrAlreadyStarted.
func Configure(cfg Config) error {
	return defaultConfig.apply(cfg)
}

// Configure decodes a JSON configuration string (DTM MicroService
// DriverOptions) with strict unknown-field rejection and applies it.
func (d *Driver) Configure(raw string) error {
	var cfg Config
	dec := json.NewDecoder(stringsReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("dtm-driver-kitex: invalid driver options: %w", err)
	}
	return defaultConfig.apply(cfg)
}
