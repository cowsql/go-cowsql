//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/example/query"
	"github.com/cowsql/go-cowsql/cluster/example/tls"
)

// Cluster is an implementation of the COWSQL database.
type Cluster struct {
	sync.RWMutex

	db         transaction.DBTX
	internalDB *DB
}

// NewCluster creates a new Cluster object.
func NewCluster(ctx context.Context, db *DB) (*Cluster, error) {
	c := &Cluster{internalDB: db}
	c.db = transaction.Enable(c.internalDB)

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		return initGlobalSchema(ctx, tx)
	})
	if err != nil {
		return nil, err
	}

	return c, nil
}

// DBTX implements [transaction.Transactor].
// Returns the database executor. DBTX is an abstraction that may be a transaction a db.
func (c *Cluster) DBTX() transaction.DBTX {
	return c.db
}

// MaxRetries implements [transaction.Transactor].
// Number of times to attempt to rollback and retry transactions.
func (c *Cluster) MaxRetries() int {
	return 250
}

// EnterExclusive implements [transaction.Transactor].
// Marks the transactor for exclusive access. Other transactions will block before proceeding to BeginTx.
// Here, we try to acquire a RW Lock for write access for 20s, or fail with an error.
func (c *Cluster) EnterExclusive() error {
	slog.Debug("Acquiring exclusive lock on cluster db")

	ch := make(chan struct{})

	go func() {
		c.Lock()
		ch <- struct{}{}
	}()

	timeout := 20 * time.Second
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout (%s)", timeout)
	}
}

// OnTxStart implements [transaction.Transactor].
// This is called on the initial call to transaction.Do against the provided function body.
//
// - Acquire read lock to mark non-exclusive access.
// - Transaction callback: set a 30s timeout on the outer transaction body, just to ensure no connection issues cause a lock-up.
// - On transaction commit/cancel, release the exclusivity lock and cancel the transaction context.
func (c *Cluster) OnTxStart(exclusive bool, f func(ctx context.Context) error) (func(ctx context.Context) error, func()) {
	var cancel context.CancelFunc

	cleanup := func() {
		if cancel != nil {
			cancel()
		}

		if exclusive {
			c.Unlock()
		} else {
			c.RUnlock()
		}
	}

	if !exclusive {
		c.RLock()
	}

	return func(ctx context.Context) error {
		var timeoutCtx context.Context

		timeoutCtx, cancel = context.WithTimeout(ctx, time.Second*30) //nolint:gosec

		return f(timeoutCtx)
	}, cleanup
}

// OnTxStartForce implements [transaction.Transactor].
// This is called on the initial call to transaction.ForceTx against the provided function body.
//
// - Acquire read lock to mark non-exclusive access.
// - Transaction callback: set a 30s timeout on the outer transaction body, just to ensure no connection issues cause a lock-up.
// - On transaction commit/cancel, release the exclusivity lock and cancel the transaction context.
func (c *Cluster) OnTxStartForce(exclusive bool, f func(ctx context.Context, tx transaction.TX) error) (func(ctx context.Context, tx transaction.TX) error, func()) {
	var cancel context.CancelFunc

	cleanup := func() {
		if cancel != nil {
			cancel()
		}

		if exclusive {
			c.Unlock()
		} else {
			c.RUnlock()
		}
	}

	if !exclusive {
		c.RLock()
	}

	return func(ctx context.Context, tx transaction.TX) error {
		var timeoutCtx context.Context

		timeoutCtx, cancel = context.WithTimeout(ctx, time.Second*30) //nolint:gosec

		return f(timeoutCtx, tx)
	}, cleanup
}

// DB implements [db.Cluster].
// Returns the underlying cowsql sql.DB, used for cleaning up the gateway.
func (c *Cluster) DB() *sql.DB {
	return c.internalDB.DB
}

// BootstrapNode implements [db.Cluster].
// Sets the name and address of the first cluster member, with id: 1.
func (c *Cluster) BootstrapNode(ctx context.Context, serverName string, clusterAddress string) error {
	return transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		result, err := tx.ExecContext(ctx, "UPDATE nodes SET name=?, address=? WHERE id=1", serverName, clusterAddress)
		if err != nil {
			return err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if n != 1 {
			return fmt.Errorf("query updated %d rows instead of 1", n)
		}

		return nil
	})
}

// CreateNode implements [db.Cluster].
// Creates the cluster member record in the global database with the given properties.
func (c *Cluster) CreateNode(ctx context.Context, name string, address string, arch int) (int64, error) {
	var id int64

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		var err error

		columns := []string{"name", "address", "schema", "api_extensions", "architecture"}
		// This example always uses 1/1/1 for schema, api_extensions, architecture.
		// These should be properly handled in real-world setups that deal with breaking changes and multiple architectures.
		values := []any{name, address, 1, 1, 1}
		id, err = query.UpsertObject(ctx, tx, "nodes", columns, values)

		return err
	})
	if err != nil {
		return -1, fmt.Errorf("Failed to create cluster member record: %w", err)
	}

	return id, nil
}

// GetNodeOfflineThreshold implements [db.Cluster].
// For this example, just return a static value. You may want this to be configurable and stored in the database.
func (c *Cluster) GetNodeOfflineThreshold(ctx context.Context) (time.Duration, error) {
	return 10 * time.Second, nil
}

// GetNodes implements [db.Cluster].
// Fetch the cluster member records.
func (c *Cluster) GetNodes(ctx context.Context) ([]db.NodeInfo, error) {
	var nodes []db.NodeInfo

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		scan := func(scan func(dest ...any) error) error {
			var node db.NodeInfo

			err := scan(&node.ID, &node.Address, &node.Name, &node.Heartbeat, &node.State, &node.Schema, &node.APIExtensions)
			if err != nil {
				return err
			}

			nodes = append(nodes, node)

			return nil
		}
		stmt := "SELECT id, address, name, heartbeat, state, schema, api_extensions from nodes WHERE state != ?"

		return query.SelectObjects(ctx, tx, stmt, scan, 1)
	})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// GetNodesCount implements [db.Cluster].
func (c *Cluster) GetNodesCount(ctx context.Context) (int, error) {
	var count int

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		var err error

		count, err = query.Count(ctx, tx, "nodes", "")

		return err
	})
	if err != nil {
		return -1, err
	}

	return count, nil
}

// GetNodesFailureDomains implements [db.Cluster].
// Returns a map associating each node address with its failure domain code.
func (c *Cluster) GetNodesFailureDomains(ctx context.Context) (map[string]uint64, error) {
	nodes, err := c.GetNodes(ctx)
	if err != nil {
		return nil, err
	}

	fds := make(map[string]uint64, len(nodes))
	for _, n := range nodes {
		fds[n.Address] = 0
	}

	return fds, nil
}

// GetNodeByAddress implements [db.Cluster].
func (c *Cluster) GetNodeByAddress(ctx context.Context, address string, pending bool) (db.NodeInfo, error) {
	value := 0
	if pending {
		value = 1
	}

	var nodes []db.NodeInfo

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		scan := func(scan func(dest ...any) error) error {
			var node db.NodeInfo

			err := scan(&node.ID, &node.Address, &node.Name, &node.Heartbeat, &node.State)
			if err != nil {
				return err
			}

			nodes = append(nodes, node)

			return nil
		}
		stmt := "SELECT id, address, name, heartbeat, state from nodes WHERE state = ? AND address = ?"

		return query.SelectObjects(ctx, tx, stmt, scan, value, address)
	})
	if err != nil {
		return db.NodeInfo{}, err
	}

	if len(nodes) != 1 {
		return db.NodeInfo{}, fmt.Errorf("Cluster member with address %q not found with pending state %v", address, pending)
	}

	return nodes[0], nil
}

// GetNodeByName implements [db.Cluster].
func (c *Cluster) GetNodeByName(ctx context.Context, name string, pending bool) (db.NodeInfo, error) {
	value := 0
	if pending {
		value = 1
	}

	var nodes []db.NodeInfo

	err := transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		scan := func(scan func(dest ...any) error) error {
			var node db.NodeInfo

			err := scan(&node.ID, &node.Address, &node.Name, &node.Heartbeat, &node.State)
			if err != nil {
				return err
			}

			nodes = append(nodes, node)

			return nil
		}
		stmt := "SELECT id, address, name, heartbeat, state from nodes WHERE state = ? AND name = ?"

		return query.SelectObjects(ctx, tx, stmt, scan, value, name)
	})
	if err != nil {
		return db.NodeInfo{}, err
	}

	if len(nodes) != 1 {
		return db.NodeInfo{}, fmt.Errorf("Pending cluster member with name %q not found with pending state %v", name, pending)
	}

	return nodes[0], nil
}

// SetNodePendingFlag implements [db.Cluster].
// Marks the cluster member record as pending / non-pending, to record cluster join state.
func (c *Cluster) SetNodePendingFlag(ctx context.Context, nodeID int64, flag bool) error {
	value := 0
	if flag {
		value = 1
	}

	return transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		result, err := tx.ExecContext(ctx, "UPDATE nodes SET state=? WHERE id=?", value, nodeID)
		if err != nil {
			return err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if n != 1 {
			return fmt.Errorf("query updated %d rows instead of 1", n)
		}

		return nil
	})
}

// RemoveNode implements [db.Cluster].
func (c *Cluster) RemoveNode(ctx context.Context, name string) error {
	return transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM nodes WHERE name=?", name)
		if err != nil {
			return err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if n != 1 {
			return fmt.Errorf("query deleted %d rows instead of 1", n)
		}

		return nil
	})
}

// SetNodeHeartbeat implements [db.Cluster].
// Sets the heartbeat time for the cluster member record with the given address.
func (c *Cluster) SetNodeHeartbeat(ctx context.Context, address string, heartbeatTime time.Time) error {
	return transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		stmt := "UPDATE nodes SET heartbeat=? WHERE address=?"

		result, err := tx.ExecContext(ctx, stmt, heartbeatTime, address)
		if err != nil {
			return err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if n != 1 {
			return fmt.Errorf("Expected to update one row and not %d", n)
		}

		return nil
	})
}

// SetNodeCertificateByName implements [db.Cluster].
// Adds the serverCert to the DB trusted certificates store using the serverName.
func (c *Cluster) SetNodeCertificateByName(ctx context.Context, serverName string, serverCert *x509.Certificate) error {
	return transaction.ForceTx(ctx, c, func(ctx context.Context, tx transaction.TX) error {
		stmt := "UPDATE nodes SET certificate=? WHERE name=?"

		result, err := tx.ExecContext(ctx, stmt, tls.Certificate{Certificate: serverCert}, serverName)
		if err != nil {
			return err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if n != 1 {
			return fmt.Errorf("Expected to update one row and not %d", n)
		}

		return nil
	})
}

// NodeIsOutdated implements [db.Cluster].
// Returns true if there's some cluster node having an API or
// schema version greater than the node this method is invoked on.
// Version management won't be implemented in this example, so neither will this method.
func (c *Cluster) NodeIsOutdated(ctx context.Context) (bool, error) {
	return false, nil
}

var _ db.Cluster = &Cluster{}
