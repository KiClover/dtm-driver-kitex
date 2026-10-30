package dtmkitex

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCredentialFile writes content to a temp YAML file and returns its path.
func writeCredentialFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write credential file: %v", err)
	}
	return path
}

const validCredentialsYAML = `profiles:
  etcd:
    dtm-prod:
      user: u-etcd
      password: p-etcd
  consul:
    dtm-prod:
      token: t-consul
    consul-only:
      token: t-consul-only
  nacos:
    dtm-prod:
      username: u-nacos
      password: p-nacos
      namespace: public
`

func TestLoadCredentialProfiles(t *testing.T) {
	set, err := loadCredentialProfiles(writeCredentialFile(t, validCredentialsYAML))
	if err != nil {
		t.Fatalf("loadCredentialProfiles: %v", err)
	}

	cases := []struct {
		scheme   string
		profile  string
		wantUser string
		wantPass string
	}{
		{SchemeEtcd, "dtm-prod", "u-etcd", "p-etcd"},
		{SchemeNacos, "dtm-prod", "u-nacos", "p-nacos"},
	}
	for _, tc := range cases {
		got, err := set.lookup(tc.scheme, tc.profile)
		if err != nil {
			t.Fatalf("lookup(%s,%s): %v", tc.scheme, tc.profile, err)
		}
		if got.User != tc.wantUser || got.Password != tc.wantPass {
			t.Fatalf("lookup(%s,%s) = %+v", tc.scheme, tc.profile, got)
		}
	}

	consul, err := set.lookup(SchemeConsul, "dtm-prod")
	if err != nil {
		t.Fatalf("lookup consul: %v", err)
	}
	if consul.Token != "t-consul" {
		t.Fatalf("consul token = %q", consul.Token)
	}
}

func TestLookupProfileErrors(t *testing.T) {
	set, err := loadCredentialProfiles(writeCredentialFile(t, validCredentialsYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := set.lookup(SchemeEtcd, "missing"); err == nil {
		t.Fatal("unknown profile must fail")
	}
	if _, err := set.lookup("kitex-unknown", "dtm-prod"); err == nil {
		t.Fatal("unknown backend must fail")
	}
	// consul-only exists under consul but must not be visible under etcd.
	if _, err := set.lookup(SchemeEtcd, "consul-only"); err == nil {
		t.Fatal("profile defined for another backend must fail")
	}
}

func TestLoadCredentialProfilesMissingFile(t *testing.T) {
	if _, err := loadCredentialProfiles(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("missing credential file must fail")
	}
}

func TestLoadCredentialProfilesUnreadablePath(t *testing.T) {
	// A directory path forces a read error without relying on chmod, which is
	// unreliable on Windows.
	if _, err := loadCredentialProfiles(t.TempDir()); err == nil {
		t.Fatal("directory path must fail")
	}
}

func TestLoadCredentialProfilesInvalidYAML(t *testing.T) {
	if _, err := loadCredentialProfiles(writeCredentialFile(t, "profiles: [not-a-map")); err == nil {
		t.Fatal("invalid YAML must fail")
	}
}

func TestLoadCredentialProfilesDuplicateProfile(t *testing.T) {
	duplicate := `profiles:
  etcd:
    dtm-prod:
      user: a
    dtm-prod:
      user: b
`
	if _, err := loadCredentialProfiles(writeCredentialFile(t, duplicate)); err == nil {
		t.Fatal("duplicate profile must fail")
	}
}

func TestCredentialErrorsDoNotLeakSecrets(t *testing.T) {
	set, err := loadCredentialProfiles(writeCredentialFile(t, validCredentialsYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = set.lookup(SchemeEtcd, "missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "p-etcd") {
		t.Fatalf("error leaks secret: %v", err)
	}
}
