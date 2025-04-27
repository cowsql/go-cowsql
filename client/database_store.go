//go:build !nosqlite3

package client

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3" // Go SQLite bindings
)

// Option that can be used to tweak node store parameters.
type NodeStoreOption func(*nodeStoreOptions)

type nodeStoreOptions struct {
	Where string
}

// DatabaseNodeStore persists a list addresses of cowsql nodes in a SQL table.
type DatabaseNodeStore struct {
	db     *sql.DB // Database handle to use.
	schema string  // Name of the schema holding the servers table.
	table  string  // Name of the servers table.
	column string  // Column name in the servers table holding the server address.
	where  string  // Optional WHERE filter
}

// DefaultNodeStore creates a new NodeStore using the given filename.
//
// If the filename ends with ".yaml" then the YamlNodeStore implementation will
// be used. Otherwise the SQLite-based one will be picked, with default names
// for the schema, table and column parameters.
//
// It also creates the table if it doesn't exist yet.
func DefaultNodeStore(filename string) (NodeStore, error) {
	if strings.HasSuffix(filename, ".yaml") {
		return NewYamlNodeStore(filename)
	}

	// Open the database.
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Since we're setting SQLite single-thread mode, we need to have one
	// connection at most.
	db.SetMaxOpenConns(1)

	// Create the servers table if it does not exist yet.
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS servers (address TEXT, UNIQUE(address))")
	if err != nil {
		return nil, fmt.Errorf("failed to create servers table: %w", err)
	}

	store := NewNodeStore(db, "main", "servers", "address")

	return store, nil
}

// NewNodeStore creates a new NodeStore.
func NewNodeStore(db *sql.DB, schema, table, column string, options ...NodeStoreOption) *DatabaseNodeStore {
	o := &nodeStoreOptions{}
	for _, option := range options {
		option(o)
	}

	return &DatabaseNodeStore{
		db:     db,
		schema: schema,
		table:  table,
		column: column,
		where:  o.Where,
	}
}

// WithNodeStoreWhereClause configures the node store to append the given
// hard-coded where clause to the SELECT query used to fetch nodes. Only the
// clause itself must be given, without the "WHERE" prefix.
func WithNodeStoreWhereClause(where string) NodeStoreOption {
	return func(options *nodeStoreOptions) {
		options.Where = where
	}
}

// Get the current servers.
func (d *DatabaseNodeStore) Get(ctx context.Context) ([]NodeInfo, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf("SELECT %s FROM %s.%s", d.column, d.schema, d.table)
	if d.where != "" {
		query += " WHERE " + d.where
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query servers table: %w", err)
	}
	defer rows.Close()

	servers := make([]NodeInfo, 0)
	for rows.Next() {
		var address string
		err := rows.Scan(&address)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch server address: %w", err)
		}
		servers = append(servers, NodeInfo{ID: 1, Address: address})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("result set failure: %w", err)
	}

	return servers, nil
}

// Set the servers addresses.
func (d *DatabaseNodeStore) Set(ctx context.Context, servers []NodeInfo) error {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	query := fmt.Sprintf("DELETE FROM %s.%s", d.schema, d.table)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to delete existing servers rows: %w", err)
	}

	query = fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES (?)", d.schema, d.table, d.column)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, server := range servers {
		if _, err := stmt.ExecContext(ctx, server.Address); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert server %s: %w", server.Address, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
