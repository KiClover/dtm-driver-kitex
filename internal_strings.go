package dtmkitex

import "strings"

// stringsReader is a tiny alias to keep config.go free of direct io concerns.
func stringsReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
