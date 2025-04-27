//go:build !nosqlite3
// +build !nosqlite3

package cowsql

import (
	"fmt"
	"os"

	"github.com/cowsql/go-cowsql/internal/bindings"
	"github.com/cowsql/go-cowsql/internal/protocol"
)

// ConfigMultiThread sets the threading mode of SQLite to Multi-thread.
//
// By default go-cowsql configures SQLite to Single-thread mode, because the
// cowsql engine itself is single-threaded, and enabling Multi-thread or
// Serialized modes would incur in a performance penality.
//
// If your Go process also uses SQLite directly (e.g. using the
// github.com/mattn/go-sqlite3 bindings) you might need to switch to
// Multi-thread mode in order to be thread-safe.
//
// IMPORTANT: It's possible to successfully change SQLite's threading mode only
// if no SQLite APIs have been invoked yet (e.g. no database has been opened
// yet). Therefore you'll typically want to call ConfigMultiThread() very early
// in your process setup. Alternatively you can set the GO_COWSQL_MULTITHREAD
// environment variable to 1 at process startup, in order to prevent go-cowsql
// from setting Single-thread mode at all.
func ConfigMultiThread() error {
	if err := bindings.ConfigMultiThread(); err != nil {
		if err, ok := err.(protocol.Error); ok && err.Code == 21 /* SQLITE_MISUSE */ {
			return fmt.Errorf("SQLite is already initialized")
		}
		return fmt.Errorf("unknow error: %w", err)
	}
	return nil
}

func init() {
	// Don't enable single thread mode by default if GO_COWSQL_MULTITHREAD
	// is set.
	if os.Getenv("GO_COWSQL_MULTITHREAD") == "1" {
		return
	}
	err := bindings.ConfigSingleThread()
	if err != nil {
		panic(fmt.Errorf("set single thread mode: %w", err))
	}
}
