package db

import (
	"context"
	"crypto/x509"
	"database/sql"
	"time"

	"github.com/cowsql/go-cowsql/cluster/db/transaction"
)

// Cluster represents the implementation for fetching cluster members from the global COWSQL database.
type Cluster interface {
	transaction.Transactor

	DB() *sql.DB

	// GetNodes returns all cluster member node info.
	GetNodes(ctx context.Context) ([]NodeInfo, error)

	// GetNodesCount returns the count of cluster member node information.
	GetNodesCount(ctx context.Context) (int, error)

	// RemoveNode removes the node with the given name.
	RemoveNode(ctx context.Context, name string) error

	// GetNodesFailureDomains returns a map associating each node address with its
	// failure domain code.
	GetNodesFailureDomains(ctx context.Context) (map[string]uint64, error)

	// GetNodeOfflineThreshold returns the amount of time that needs to elapse after
	// which a series of unsuccessful heartbeat will make the node be considered
	// offline.
	GetNodeOfflineThreshold(ctx context.Context) (time.Duration, error)

	// NodeIsOutdated returns true if there's some cluster node having an API or
	// schema version greater than the node this method is invoked on.
	NodeIsOutdated(ctx context.Context) (bool, error)

	// SetNodeHeartbeat updates the heartbeat column of the node with the given address.
	SetNodeHeartbeat(ctx context.Context, address string, heartbeatTime time.Time) error

	// GetNodeByAddress returns the node with the given network address and pending state.
	GetNodeByAddress(ctx context.Context, address string, pending bool) (NodeInfo, error)

	// GetNodeByName returns the node with the given name and pending state.
	GetNodeByName(ctx context.Context, name string, pending bool) (NodeInfo, error)

	// BootstrapNode sets the name and address of the first cluster member, with id: 1.
	BootstrapNode(ctx context.Context, serverName string, clusterAddress string) error

	// CreateNode adds a node to the current list of members that are part of the
	// cluster. The node's architecture should be the architecture of the machine the
	// method is being run on. It returns the ID of the newly inserted row.
	CreateNode(ctx context.Context, name, address string, arch int) (int64, error)

	// SetNodePendingFlag toggles the pending flag for the node. A node is pending when
	// it's been accepted in the cluster, but has not yet actually joined it.
	SetNodePendingFlag(ctx context.Context, nodeID int64, flag bool) error

	// SetNodeCertificateByName adds the serverCert to the DB trusted certificates store using the serverName.
	SetNodeCertificateByName(ctx context.Context, serverName string, serverCert *x509.Certificate) error
}

// ClusterWarningHandler represents the implementation for managing dedicated warnings.
type ClusterWarningHandler interface {
	// ResolveOfflineMemberWarning resolves offline member warnings for the given nodeID and serverName.
	ResolveOfflineMemberWarning(ctx context.Context, nodeID int64, serverName string) error

	// EmitOfflineMemberWarning emits an offline member warning for the given nodeID and serverName.
	EmitOfflineMemberWarning(ctx context.Context, nodeID int64, serverName string, msg string) error

	// ResolveTimeSkewWarning resolves time skew warnings for the given serverName.
	ResolveTimeSkewWarning(ctx context.Context, serverName string) error

	// EmitTimeSkewWarning emits a time skew warning for the given serverName.
	EmitTimeSkewWarning(ctx context.Context, serverName string, msg string) error
}

// ClusterExternal represents the implementation for managing external cluster resources.
type ClusterExternal[T any] interface {
	// GetLocalResources Fetches external data of the type from the cluster DB.
	GetLocalResources(ctx context.Context) (T, error)

	// UpdateClusterResources updates external data of the given type from the cluster DB.
	UpdateClusterResources(ctx context.Context, node NodeInfo, t T) error

	// ClearNode removes all data referencing the nodeID.
	ClearNode(ctx context.Context, nodeID int64) error

	// NodeIsEmpty returns an empty string if the node with the given ID has no
	// referencing data associated with it. Otherwise, it returns a message
	// say what's left.
	NodeIsEmpty(ctx context.Context, nodeID int64) (string, error)
}
