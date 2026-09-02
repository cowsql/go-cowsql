package db

import (
	"context"
	"database/sql"

	"github.com/cowsql/go-cowsql/cluster/db/transaction"
)

// Node represents the implementation for fetching cached cluster members from the local database.
type Node interface {
	transaction.Transactor

	// DB returns the underlying DB.
	DB() *sql.DB

	// LogPath returns the path to the database logs.
	LogPath() string

	// Dir returns the containing directory of the database file.
	Dir() string

	// GetRaftNodes returns all cached cluster member raft node info.
	GetRaftNodes(ctx context.Context) ([]RaftNode, error)

	// GetRaftNodeAddresses returns the addresses for all cached cluster members.
	GetRaftNodeAddresses(ctx context.Context) ([]string, error)

	// GetRaftNodeAddress returns the address for the cached cluster member node info matching the node id.
	GetRaftNodeAddress(ctx context.Context, id int64) (string, error)

	// CreateFirstRaftNode adds a the first node of the cluster. It ensures that the
	// database ID is 1, to match the server ID of the first raft log entry.
	//
	// This method is supposed to be called when there are no rows in raft_nodes,
	// and it will replace whatever existing row has ID 1.
	CreateFirstRaftNode(ctx context.Context, address string, name string) error

	// CreateRaftNode adds a node to the current list of nodes that are part of the
	// cowsql Raft cluster. It returns the ID of the newly inserted row.
	CreateRaftNode(ctx context.Context, address string, name string) (int64, error)

	// RemoveRaftNode removes a node from the current list of nodes that are
	// part of the cowsql Raft cluster.
	RemoveRaftNode(ctx context.Context, id int64) error

	// ReplaceRaftNodes replaces the current list of raft nodes.
	ReplaceRaftNodes(ctx context.Context, nodes []RaftNode) error

	// DetermineRaftNode figures out what raft node ID and address we have, if any.
	DetermineRaftNode(ctx context.Context) (*RaftNode, error)

	// GetClusterAddress fetches the current cluster address used for intra-cluster communication.
	GetClusterAddress(ctx context.Context) (string, error)

	// SetClusterAddress sets the cluster address used for intra-cluster communication.
	SetClusterAddress(ctx context.Context, address string) error
}
