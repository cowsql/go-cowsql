//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/mattn/go-sqlite3"

	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/driver"
)

func sqliteEnableForeignKeys(conn *sqlite3.SQLiteConn) error {
	_, err := conn.Exec("PRAGMA foreign_keys=ON;", nil)

	return err
}

func init() {
	sql.Register("cowsql_local_sqlite3", &sqlite3.SQLiteDriver{ConnectHook: sqliteEnableForeignKeys})
}

// DB is a wrapper around sql.DB that implements BeginTx.
type DB struct {
	*sql.DB
}

// BeginTx implements [transaction.DB].
func (n *DB) BeginTx(ctx context.Context) (transaction.TX, error) {
	return n.DB.BeginTx(ctx, nil)
}

// OpenLocalDB opens the local database object.
func OpenLocalDB(dir string) (*DB, error) {
	path := filepath.Join(dir, "local.db")

	// These are used to tune the transaction BEGIN behavior instead of using the
	// similar "locking_mode" pragma (locking for the whole database connection).
	openPath := fmt.Sprintf("%s?_busy_timeout=%d&_txlock=exclusive", path, 5000)

	// Open the database. If the file doesn't exist it is created.
	db, err := sql.Open("cowsql_local_sqlite3", openPath)
	if err != nil {
		return nil, fmt.Errorf("Cannot open node database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &DB{DB: db}, nil
}

// Monotonic serial number for registering new instances of cowsql.Driver
// using the database/sql stdlib package. This is needed since there's no way
// to unregister drivers, and in unit tests more than one driver gets
// registered.
var cowsqlDriverSerial uint64

func cowsqlDriverName() string {
	defer atomic.AddUint64(&cowsqlDriverSerial, 1)

	return fmt.Sprintf("cowsql-%d", cowsqlDriverSerial)
}

// OpenClusterDB opens the cluster database object.
//
// The dialer argument is a function that returns a gRPC dialer that can be
// used to connect to a database node using the gRPC SQL package.
func OpenClusterDB(closingCtx context.Context, drv *driver.Driver, dir string, timeout time.Duration) (*DB, error) {
	err := os.MkdirAll(filepath.Join(dir, "global"), 0o755)
	if err != nil {
		return nil, err
	}

	driverName := cowsqlDriverName()
	sql.Register(driverName, drv)

	// Create the cluster db. This won't immediately establish any network
	// connection, that will happen only when a db transaction is started
	// (see the database/sql connection pooling code for more details).
	db, err := sql.Open(driverName, "db.bin")
	if err != nil {
		return nil, fmt.Errorf("Failed to open global database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Test that the cluster database is operational. We wait up to the
	// given timeout , in case there's no quorum of nodes online yet.
	connectCtx, connectCancel := context.WithTimeout(closingCtx, timeout)
	defer connectCancel()

	for i := 0; ; i++ {
		log := slog.Default()
		log.Info("Connecting to global database")

		pingCtx, pingCancel := context.WithTimeout(connectCtx, time.Second*5)
		err = db.PingContext(pingCtx)

		pingCancel()

		log = slog.With("error", err, "attempt", i)
		if err != nil && !errors.Is(err, driver.ErrNoAvailableLeader) {
			return nil, err
		} else if err == nil {
			slog.Info("Connected to global database")

			break
		}

		log.Error("Failed connecting to global database")

		select {
		case <-connectCtx.Done():
			return nil, connectCtx.Err()
		default:
			time.Sleep(2 * time.Second)
		}
	}

	_, err = db.ExecContext(closingCtx, "PRAGMA cache_size=-50000")
	if err != nil {
		return nil, fmt.Errorf("Failed to set page cache size: %w", err)
	}

	return &DB{DB: db}, nil
}
