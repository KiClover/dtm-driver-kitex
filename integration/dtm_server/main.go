// dtm_server runs a real DTM server wired with the Kitex driver, so the
// integration tests can drive end-to-end Saga flows (raw payload replay,
// barrier semantics) against a genuine dtmsvr.
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/dtm-labs/dtm/dtmsvr/entry"
	_ "github.com/dtm-labs/dtm/dtmsvr/microservices"
	_ "github.com/dtm-labs/dtmdriver-kitex"
)

func main() {
	version := "dtm-kitex-test"
	app, conf := entry.Main(&version)
	if app == nil {
		return
	}
	q := make(chan os.Signal, 1)
	signal.Notify(q, syscall.SIGINT, syscall.SIGTERM)
	<-q
	_ = conf
}
