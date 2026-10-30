package dtmkitex

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// credentialProfile is one backend/profile entry from the credential file.
type credentialProfile struct {
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Token     string `yaml:"token"`
	Username  string `yaml:"username"`
	Namespace string `yaml:"namespace"`

	TLSCA   string `yaml:"tls_ca"`
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
}

// credentialSet is the immutable, loaded credential file.
type credentialSet struct {
	profiles map[string]map[string]credentialProfile
}

// credentialFile is the on-disk YAML shape.
type credentialFile struct {
	Profiles map[string]map[string]credentialProfile `yaml:"profiles"`
}

// loadCredentialProfiles reads and validates a credential YAML file. All file
// reads and TLS material parsing happen before the snapshot is published.
func loadCredentialProfiles(path string) (*credentialSet, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: read credential file: %w", err)
	}
	var file credentialFile
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: invalid credential file: %w", err)
	}
	if len(file.Profiles) == 0 {
		return nil, fmt.Errorf("dtm-driver-kitex: credential file has no profiles")
	}
	// Validate TLS material eagerly so a bad file fails startup, never a call.
	for scheme, profiles := range file.Profiles {
		for name, profile := range profiles {
			if _, err := buildTLSConfig(profile); err != nil {
				return nil, fmt.Errorf("dtm-driver-kitex: profile %s/%s: %w", scheme, name, err)
			}
		}
	}
	return &credentialSet{profiles: file.Profiles}, nil
}

// lookup returns the profile for a backend scheme.
func (s *credentialSet) lookup(scheme, profile string) (credentialProfile, error) {
	if s == nil {
		return credentialProfile{}, fmt.Errorf("dtm-driver-kitex: no credential file configured")
	}
	backend := backendName(scheme)
	byName, ok := s.profiles[backend]
	if !ok {
		return credentialProfile{}, fmt.Errorf("dtm-driver-kitex: no credential profiles for backend %q", backend)
	}
	p, ok := byName[profile]
	if !ok {
		return credentialProfile{}, fmt.Errorf("dtm-driver-kitex: credential profile %q not found for backend %q", profile, backend)
	}
	return normalizeProfile(p), nil
}

// normalizeProfile keeps a single username field for all backends.
func normalizeProfile(p credentialProfile) credentialProfile {
	if p.User == "" {
		p.User = p.Username
	}
	return p
}

// backendName maps a URI scheme onto its credential-file backend key.
func backendName(scheme string) string {
	return strings.TrimPrefix(scheme, "kitex-")
}

// buildTLSConfig converts profile TLS paths into a strict TLS configuration.
// An empty profile yields a nil config (plaintext). Any TLS error is fatal: the
// caller must never fall back to plaintext.
func buildTLSConfig(p credentialProfile) (*tls.Config, error) {
	if p.TLSCA == "" && p.TLSCert == "" && p.TLSKey == "" {
		return nil, nil
	}
	if (p.TLSCert == "") != (p.TLSKey == "") {
		return nil, fmt.Errorf("tls_cert and tls_key must be set together")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if p.TLSCA != "" {
		pem, err := os.ReadFile(p.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("read tls_ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls_ca contains no valid certificates")
		}
		cfg.RootCAs = pool
	}
	if p.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(p.TLSCert, p.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// inlineSecretKeys are the query keys that carry credentials.
var inlineSecretKeys = []string{"user", "password", "token", "username"}

// resolveCredentials merges URI inline credentials and the credential profile
// according to the frozen configuration.
func (s *configState) resolveCredentials(dc *discoveryConfig) (credentialProfile, error) {
	s.mu.RLock()
	cfg := s.cfg
	set := s.creds
	s.mu.RUnlock()

	hasInline := false
	for _, key := range inlineSecretKeys {
		if dc.query.Get(key) != "" {
			hasInline = true
			break
		}
	}
	profileName := dc.query.Get("credential_profile")

	switch {
	case hasInline && profileName != "":
		return credentialProfile{}, fmt.Errorf("dtm-driver-kitex: target must not combine credential_profile with inline credentials")
	case hasInline && !cfg.AllowInlineCredentials:
		return credentialProfile{}, fmt.Errorf("dtm-driver-kitex: inline credentials are disabled; use credential_profile")
	case hasInline:
		return inlineCredentials(dc), nil
	case profileName != "":
		return set.lookup(dc.scheme, profileName)
	default:
		return credentialProfile{}, nil
	}
}

// inlineCredentials maps URI query keys onto a credential profile.
func inlineCredentials(dc *discoveryConfig) credentialProfile {
	p := credentialProfile{
		User:     dc.query.Get("user"),
		Password: dc.query.Get("password"),
		Token:    dc.query.Get("token"),
		Username: dc.query.Get("username"),
	}
	if p.User == "" {
		p.User = p.Username
	}
	return p
}
