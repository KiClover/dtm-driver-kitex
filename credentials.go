package dtmkitex

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// credentialProfile contains only credentials accepted in registry URIs.
// Secrets are never included directly in cache keys or error messages.
type credentialProfile struct {
	User     string
	Password string
	Token    string
}

func inlineCredentials(dc *discoveryConfig) credentialProfile {
	user := dc.query.Get("user")
	if user == "" {
		user = dc.query.Get("username")
	}
	return credentialProfile{
		User:     user,
		Password: dc.query.Get("password"),
		Token:    dc.query.Get("token"),
	}
}

func (p credentialProfile) cacheKey() string {
	var encoded []byte
	for _, value := range []string{p.User, p.Password, p.Token} {
		encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(value)))
		encoded = append(encoded, value...)
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest)
}
