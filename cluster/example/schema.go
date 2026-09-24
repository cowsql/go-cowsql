//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"fmt"

	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/example/query"
)

func initLocalSchema(ctx context.Context, tx transaction.DBTX) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS raft_nodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  address TEXT NOT NULL,
  role INTEGER NOT NULL DEFAULT 0,
  name TEXT NOT NULL default "",
  certificate TEXT,
  UNIQUE (address)
);
  `)
	if err != nil {
		return fmt.Errorf("Failed to initialize local database: %w", err)
	}

	return nil
}

func initGlobalSchema(ctx context.Context, tx transaction.DBTX) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS nodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  schema INTEGER NOT NULL,
  api_extensions INTEGER NOT NULL,
  heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
  state INTEGER NOT NULL DEFAULT 0,
  architecture INTEGER NOT NULL DEFAULT 0 CHECK (architecture > 0),
  certificate TEXT,
  UNIQUE (name),
  UNIQUE (address)
);`)
	if err != nil {
		return fmt.Errorf("Failed to initialize global database: %w", err)
	}

	count, err := query.Count(ctx, tx, "nodes", "")
	if err != nil {
		return fmt.Errorf("Failed to get initial count of cluster members: %w", err)
	}

	if count == 0 {
		_, err := tx.ExecContext(ctx, `INSERT INTO nodes(id, name, address, schema, api_extensions, architecture) VALUES (1, 'none', '0.0.0.0', '1', '1', '1');`)
		if err != nil {
			return fmt.Errorf("Failed to create default uninitialized cluster member: %w", err)
		}
	}

	return nil
}
