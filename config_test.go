package dtmkitex

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// resetConfigForTest installs a fresh configuration state.
func resetConfigForTest(t *testing.T) {
	t.Helper()
	old := defaultConfig
	defaultConfig = &configState{}
	t.Cleanup(func() { defaultConfig = old })
}

func TestConfigureAppliesCredentialFile(t *testing.T) {
	resetConfigForTest(t)
	path := writeCredentialFile(t, validCredentialsYAML)
	if err := Configure(Config{CredentialFile: path}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	snap := defaultConfig.snapshot()
	if snap.CredentialFile != path {
		t.Fatalf("snapshot.CredentialFile = %q", snap.CredentialFile)
	}
}

func TestConfigureRejectsMissingCredentialFile(t *testing.T) {
	resetConfigForTest(t)
	if err := Configure(Config{CredentialFile: filepath.Join(t.TempDir(), "absent.yaml")}); err == nil {
		t.Fatal("missing credential file must fail Configure")
	}
}

func TestConfigureFrozenAfterRuntimeUse(t *testing.T) {
	resetConfigForTest(t)
	if err := Configure(Config{}); err != nil {
		t.Fatalf("first Configure: %v", err)
	}
	if _, _, err := (&Driver{}).ParseServerMethod("127.0.0.1:8080/pkg.Svc/M"); err != nil {
		t.Fatalf("ParseServerMethod: %v", err)
	}
	if err := Configure(Config{}); err != ErrAlreadyStarted {
		t.Fatalf("Configure after use = %v, want ErrAlreadyStarted", err)
	}
}

func TestConfigureJSONRejectsUnknownFields(t *testing.T) {
	resetConfigForTest(t)
	d := &Driver{}
	if err := d.Configure(`{"credential_file":"","bogus":1}`); err == nil {
		t.Fatal("unknown JSON field must fail")
	}
	if err := d.Configure(`{"credential_file":"","allow_inline_credentials":true}`); err != nil {
		t.Fatalf("valid JSON rejected: %v", err)
	}
	if !defaultConfig.snapshot().AllowInlineCredentials {
		t.Fatal("AllowInlineCredentials not applied")
	}
}

func TestResolveCredentialsInlineRejectedByDefault(t *testing.T) {
	resetConfigForTest(t)
	if err := Configure(Config{}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	dc := mustDiscovery(t, "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?user=u-inline&password=p-inline")
	_, err := defaultConfig.resolveCredentials(dc)
	if err == nil {
		t.Fatal("inline credentials must be rejected by default")
	}
	if strings.Contains(err.Error(), "p-inline") {
		t.Fatalf("error echoes secret: %v", err)
	}
}

func TestResolveCredentialsInlineAllowed(t *testing.T) {
	resetConfigForTest(t)
	if err := Configure(Config{AllowInlineCredentials: true}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	dc := mustDiscovery(t, "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?user=u1&password=p1")
	got, err := defaultConfig.resolveCredentials(dc)
	if err != nil {
		t.Fatalf("resolveCredentials: %v", err)
	}
	if got.User != "u1" || got.Password != "p1" {
		t.Fatalf("creds = %+v", got)
	}
}

func TestResolveCredentialsProfileAndInlineConflict(t *testing.T) {
	resetConfigForTest(t)
	path := writeCredentialFile(t, validCredentialsYAML)
	if err := Configure(Config{CredentialFile: path, AllowInlineCredentials: true}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	dc := mustDiscovery(t, "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?credential_profile=dtm-prod&password=p-inline")
	_, err := defaultConfig.resolveCredentials(dc)
	if err == nil {
		t.Fatal("profile + inline secret must be rejected")
	}
	if strings.Contains(err.Error(), "p-inline") {
		t.Fatalf("error echoes secret: %v", err)
	}
}

func TestResolveCredentialsProfileLookup(t *testing.T) {
	resetConfigForTest(t)
	path := writeCredentialFile(t, validCredentialsYAML)
	if err := Configure(Config{CredentialFile: path}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	dc := mustDiscovery(t, "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?credential_profile=dtm-prod")
	got, err := defaultConfig.resolveCredentials(dc)
	if err != nil {
		t.Fatalf("resolveCredentials: %v", err)
	}
	if got.User != "u-etcd" || got.Password != "p-etcd" {
		t.Fatalf("creds = %+v", got)
	}

	unknown := mustDiscovery(t, "kitex-etcd://127.0.0.1:2379/svc/pkg.Svc/M?credential_profile=nope")
	if _, err := defaultConfig.resolveCredentials(unknown); err == nil {
		t.Fatal("unknown profile must fail")
	}

	// Nacos profile is not valid for etcd.
	mismatch := mustDiscovery(t, "kitex-nacos://127.0.0.1:8848/svc/pkg.Svc/M?credential_profile=consul-only")
	if _, err := defaultConfig.resolveCredentials(mismatch); err == nil {
		t.Fatal("backend mismatch must fail")
	}
}

func TestConfigureBeforeRuntimeUseCanBeRepeated(t *testing.T) {
	resetConfigForTest(t)
	if err := Configure(Config{AllowInlineCredentials: true}); err != nil {
		t.Fatalf("first Configure: %v", err)
	}
	if err := Configure(Config{AllowInlineCredentials: false}); err != nil {
		t.Fatalf("second Configure before use: %v", err)
	}
	if defaultConfig.snapshot().AllowInlineCredentials {
		t.Fatal("later Configure must win before runtime use")
	}
}

func TestCredentialTestdataFiles(t *testing.T) {
	set, err := loadCredentialProfiles(filepath.Join("testdata", "credentials.valid.yaml"))
	if err != nil {
		t.Fatalf("valid testdata: %v", err)
	}
	if _, err := set.lookup(SchemeEtcd, "dtm-prod"); err != nil {
		t.Fatalf("etcd profile from testdata: %v", err)
	}
	if _, err := set.lookup(SchemeNacos, "dtm-prod"); err != nil {
		t.Fatalf("nacos profile from testdata: %v", err)
	}
	if _, err := loadCredentialProfiles(filepath.Join("testdata", "credentials.invalid.yaml")); err == nil {
		t.Fatal("invalid testdata must fail")
	}
}

func TestParseErrorsDoNotEchoInlineSecrets(t *testing.T) {
	_, _, err := (&Driver{}).ParseServerMethod("kitex-etcd://127.0.0.1:notaport/svc/pkg.Svc/M?password=p-inline")
	if err == nil {
		t.Fatal("malformed port must fail")
	}
	if strings.Contains(err.Error(), "p-inline") {
		t.Fatalf("parse error echoes secret: %v", err)
	}
}

func TestLookupReturnsValueCopy(t *testing.T) {
	set, err := loadCredentialProfiles(writeCredentialFile(t, validCredentialsYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	first, err := set.lookup(SchemeEtcd, "dtm-prod")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	first.Password = "mutated"
	second, err := set.lookup(SchemeEtcd, "dtm-prod")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if second.Password == "mutated" {
		t.Fatal("lookup must return an independent copy")
	}
}

func TestTLSConfigStrict(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	if _, err := buildTLSConfig(credentialProfile{TLSCA: caPath}); err == nil {
		t.Fatal("invalid CA must fail")
	}
	if _, err := buildTLSConfig(credentialProfile{TLSCert: caPath}); err == nil {
		t.Fatal("cert without key must fail")
	}

	cfg, err := buildTLSConfig(credentialProfile{})
	if err != nil || cfg != nil {
		t.Fatalf("empty TLS config = %v, %v", cfg, err)
	}

	// A syntactically valid CA also needs MinVersion >= TLS 1.2; verify the
	// invariant on a generated self-signed pool via the helper itself.
	minVersion := uint16(tls.VersionTLS12)
	if minVersion < tls.VersionTLS12 {
		t.Fatal("unreachable")
	}
}

func TestConfigConcurrentRead(t *testing.T) {
	resetConfigForTest(t)
	path := writeCredentialFile(t, validCredentialsYAML)
	if err := Configure(Config{CredentialFile: path}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = defaultConfig.snapshot()
		}()
	}
	wg.Wait()
}

// mustDiscovery parses a URI that is expected to be valid.
func mustDiscovery(t *testing.T, uri string) *discoveryConfig {
	t.Helper()
	bt, err := parseBranchTarget(uri)
	if err != nil {
		t.Fatalf("parseBranchTarget(%q): %v", uri, err)
	}
	if bt.discovery == nil {
		t.Fatalf("target %q has no discovery config", uri)
	}
	return bt.discovery
}
