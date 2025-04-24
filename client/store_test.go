//go:build !nosqlite3
// +build !nosqlite3

package client_test

import (
	"context"
	"database/sql"
	"testing"

	cowsql "github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/driver"
)

func assertEqualError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatal()
	}

	if err.Error() != msg {
		t.Fatal(err)
	}
}

// Exercise setting and getting servers in a DatabaseNodeStore created with
// DefaultNodeStore.
func TestDefaultNodeStore(t *testing.T) {
	// Create a new default store.
	store, err := client.DefaultNodeStore(":memory:")
	requireNoError(t, err)

	// Set and get some targets.
	err = store.Set(context.Background(), []client.NodeInfo{
		{Address: "1.2.3.4:666"}, {Address: "5.6.7.8:666"},
	},
	)
	requireNoError(t, err)

	servers, err := store.Get(context.Background())
	assertEqual(t, []client.NodeInfo{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(1), Address: "5.6.7.8:666"},
	},
		servers)

	// Set and get some new targets.
	err = store.Set(context.Background(), []client.NodeInfo{
		{Address: "1.2.3.4:666"}, {Address: "9.9.9.9:666"},
	})
	requireNoError(t, err)

	servers, err = store.Get(context.Background())
	assertEqual(t, []client.NodeInfo{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(1), Address: "9.9.9.9:666"},
	},
		servers)

	// Setting duplicate targets returns an error and the change is not
	// persisted.
	err = store.Set(context.Background(), []client.NodeInfo{
		{Address: "1.2.3.4:666"}, {Address: "1.2.3.4:666"},
	})
	assertEqualError(t, err, "failed to insert server 1.2.3.4:666: UNIQUE constraint failed: servers.address")

	servers, err = store.Get(context.Background())
	assertEqual(t, []client.NodeInfo{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(1), Address: "9.9.9.9:666"},
	},
		servers)
}

func TestConfigMultiThread(t *testing.T) {
	cleanup := dummyDBSetup(t)
	defer cleanup()

	err := cowsql.ConfigMultiThread()
	assertEqualError(t, err, "SQLite is already initialized")
}

func dummyDBSetup(t *testing.T) func() {
	store := client.NewInmemNodeStore()
	driver, err := driver.New(store)
	requireNoError(t, err)
	sql.Register("dummy", driver)
	db, err := sql.Open("dummy", "test.db")
	requireNoError(t, err)
	cleanup := func() {
		requireNoError(t, db.Close())
	}
	return cleanup
}
