//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/example/query"
)

// Node is an implementation of the local raft node store for COWSQL.
type Node struct {
	dir        string
	internalDB *DB
	db         transaction.DBTX

	clusterAddress string
}

// NewNode creates a new Node object.
func NewNode(ctx context.Context, db *DB, clusterAddress string, dir string) (*Node, error) {
	c := &Node{internalDB: db, clusterAddress: clusterAddress, dir: dir}
	c.db = transaction.Enable(c.internalDB)

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		return initLocalSchema(ctx, tx)
	})
	if err != nil {
		return nil, err
	}

	return c, nil
}

// DBTX implements [transaction.Transactor].
// Returns the database executor. DBTX is an abstraction that may be a transaction a db.
func (n *Node) DBTX() transaction.DBTX {
	return n.db
}

// MaxRetries implements [transaction.Transactor].
// Number of times to attempt to rollback and retry transactions.
// For the local database, there should only ever be one transaction in flight, so don't retry at all.
func (n *Node) MaxRetries() int {
	return 0
}

// EnterExclusive implements [transaction.Transactor].
// Marks the transactor for exclusive access. Other transactions will block before proceeding to BeginTx.
// For the local database, there should only ever be one transaction in flight, so we don't need to set this up.
func (n *Node) EnterExclusive() error {
	return nil
}

// OnTxStart implements [transaction.Transactor].
// This is called on the initial call to transaction.Do against the provided function body.
// Perform any pre-transaction setup here, or wrap the transaction body itself.
// Returns the transaction body and a cleanup function to return on committing / aborting the transaction.
func (n *Node) OnTxStart(exclusive bool, f func(ctx context.Context) error) (func(ctx context.Context) error, func()) {
	return f, func() {}
}

// OnTxStartForce implements [transaction.Transactor].
// This is called on the initial call to transaction.ForceTx against the provided function body.
// Perform any pre-transaction setup here, or wrap the transaction body itself.
// Returns the transaction body and a cleanup function to return on committing / aborting the transaction.
func (n *Node) OnTxStartForce(exclusive bool, f func(ctx context.Context, tx transaction.TX) error) (func(ctx context.Context, tx transaction.TX) error, func()) {
	return f, func() {}
}

// DB implements [db.Node].
// Returns the underlying sql.DB, used for managing the node store as well as handling gateway cleanup.
func (n *Node) DB() *sql.DB {
	return n.internalDB.DB
}

// GlobalDatabaseDir implements [db.Node].
// Returns the directory in which to set up the global database files.
func (n *Node) GlobalDatabaseDir() string {
	return filepath.Join(n.dir, "global")
}

// LogPath implements [db.Node].
// Returns the location of the local database logs.
func (n *Node) LogPath() string {
	return filepath.Join(n.dir, "logs.db")
}

// CreateFirstRaftNode implements [db.Node].
// Creates the first clustered raft record in the local database cache.
func (n *Node) CreateFirstRaftNode(ctx context.Context, address string, name string) error {
	return transaction.ForceTx(ctx, n, func(ctx context.Context, tx transaction.TX) error {
		id, err := query.UpsertObject(ctx, tx, "raft_nodes", []string{"id", "address", "name"}, []any{int64(1), address, name})
		if err != nil {
			return fmt.Errorf("Failed to create first raft node: %w", err)
		}

		if id != 1 {
			return errors.New("could not set raft node ID to 1")
		}

		return nil
	})
}

// DetermineRaftNode implements [db.Node].
// Determines the raft node corresponding to this system. If clustering has not yet been set up, it should return a db.DefaultRaftNode.
func (n *Node) DetermineRaftNode(ctx context.Context) (*db.RaftNode, error) {
	var node *db.RaftNode

	err := transaction.ForceTx(ctx, n, func(ctx context.Context, tx transaction.TX) error {
		nodes, err := n.GetRaftNodes(ctx)
		if err != nil {
			return err
		}

		for _, raftNode := range nodes {
			if raftNode.Address == n.clusterAddress && n.clusterAddress != "" {
				node = &raftNode

				return nil
			}
		}

		node = db.DefaultRaftNode()

		return nil
	})
	if err != nil {
		return nil, err
	}

	return node, nil
}

// GetRaftNode implements [db.Node].
// Returns the cached raft node associated with the given node ID.
func (n *Node) GetRaftNode(ctx context.Context, id int64) (*db.RaftNode, bool, error) {
	var node *db.RaftNode

	err := transaction.ForceTx(ctx, n, func(ctx context.Context, tx transaction.TX) error {
		nodes, err := n.GetRaftNodes(ctx)
		if err != nil {
			return err
		}

		for _, raftNode := range nodes {
			if raftNode.ID == uint64(id) { //nolint:gosec
				node = &raftNode

				return nil
			}
		}

		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return node, node != nil, nil
}

// GetRaftNodes implements [db.Node].
// Returns the local database cache of raft nodes.
func (n *Node) GetRaftNodes(ctx context.Context) ([]db.RaftNode, error) {
	var nodes []db.RaftNode

	err := transaction.ForceTx(ctx, n, func(ctx context.Context, tx transaction.TX) error {
		var scan query.Dest = func(scan func(dest ...any) error) error {
			var obj db.RaftNode

			err := scan(&obj.ID, &obj.Address, &obj.Name, &obj.Role)
			if err != nil {
				return err
			}

			nodes = append(nodes, obj)

			return nil
		}

		stmt := "SELECT id, address, name, role FROM raft_nodes ORDER BY id"

		return query.SelectObjects(ctx, tx, stmt, scan)
	})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// ReplaceRaftNodes implements [db.Node].
// Replaces all existing raft nodes in the local database with the new given set.
// In this example, we're also holding certificates for each cluster member in the same table, so they are persisted to the new list.
func (n *Node) ReplaceRaftNodes(ctx context.Context, nodes []db.RaftNode) error {
	return transaction.ForceTx(ctx, n, func(ctx context.Context, tx transaction.TX) error {
		certsByAddr, err := GetLocalCertificates(ctx, n)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, "DELETE from raft_nodes")
		if err != nil {
			return err
		}

		for _, node := range nodes {
			args := []any{node.ID, node.Name, node.Address, nil, node.Role}

			cert, ok := certsByAddr[node.Address]
			if ok {
				args = []any{node.ID, node.Name, node.Address, cert, node.Role}
			}

			id, err := query.UpsertObject(ctx, tx, "raft_nodes", []string{"id", "name", "address", "certificate", "role"}, args)
			if err != nil {
				return err
			}

			if id != int64(node.ID) { //nolint:gosec
				return fmt.Errorf("Insert ID %d did not match expected %d", id, node.ID)
			}
		}

		return nil
	})
}

// SetClusterAddress implements [db.Node].
// Sets the cluster address used for intra-cluster communication.
func (n *Node) SetClusterAddress(ctx context.Context, address string) error {
	n.clusterAddress = address

	return nil
}

// GetClusterAddress implements [db.Node].
// Returns the cluster address used for intra-cluster communication.
// This address is unique to each member, and must not be changed once assigned, as it is used to establish quorum on start.
func (n *Node) GetClusterAddress(ctx context.Context) (string, error) {
	return n.clusterAddress, nil
}

var _ db.Node = &Node{}
