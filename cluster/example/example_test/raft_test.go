//go:build !nosqlite3

package example_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/example/query"
	exampletls "github.com/cowsql/go-cowsql/cluster/example/tls"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
)

// Set the cluster.https_address config key to the given address, and insert the
// address into the raft_nodes table.
//
// This effectively makes the node act as a database raft node.
func setRaftRole(t *testing.T, database db.Node, address string) client.NodeStore {
	t.Helper()
	require.NoError(t, transaction.ForceTx(context.TODO(), database, func(ctx context.Context, tx transaction.TX) error {
		columns := []string{"address", "name"}
		values := []any{address, "test"}
		_, err := query.UpsertObject(ctx, tx, "raft_nodes", columns, values)

		return err
	}))

	store := client.NewNodeStore(database.DB(), "main", "raft_nodes", "address")

	return store
}

// Create a new test HTTP server configured with the given TLS certificate and
// using the given handler.
func newServer(cert cowsqltls.CertInfo, handler http.Handler) *httptest.Server {
	server := httptest.NewUnstartedServer(handler)
	server.TLS = exampletls.ServerTLSConfig(cert)
	server.Listener = exampletls.NewFancyTLSListener(server.Listener, cert)
	server.Start()

	return server
}
