package db

import (
	"context"
	"crypto/x509"
	"time"

	"github.com/cowsql/go-cowsql/cluster/db/transaction"
)

// Cluster represents the implementation for fetching cluster members from the global COWSQL database.
type Cluster interface {
	transaction.Transactor

	// GetNodes returns all cluster member node info.
	GetNodes(ctx context.Context) ([]NodeInfo, error)

	// GetNodeByName returns the node info of the cluster member with the matching name.
	GetNodeByName(ctx context.Context, name string) (NodeInfo, error)

	// GetNodesCount returns the count of cluster member node information.
	GetNodesCount(ctx context.Context) (int, error)

	// ClearNode removes all data referencing the nodeID.
	ClearNode(ctx context.Context, nodeID int64) error

	// RemoveNode removes the node with the given id.
	RemoveNode(ctx context.Context, nodeID int64) error

	// GetNodesFailureDomains returns a map associating each node address with its
	// failure domain code.
	GetNodesFailureDomains(ctx context.Context) (map[string]uint64, error)

	// GetNodeOfflineThreshold returns the amount of time that needs to elapse after
	// which a series of unsuccessful heartbeat will make the node be considered
	// offline.
	GetNodeOfflineThreshold(ctx context.Context) (time.Duration, error)

	// NodeIsEmpty returns an empty string if the node with the given ID has no
	// referencing data associated with it. Otherwise, it returns a message
	// say what's left.
	NodeIsEmpty(ctx context.Context, nodeID int64) (string, error)

	// NodeIsOutdated returns true if there's some cluster node having an API or
	// schema version greater than the node this method is invoked on.
	NodeIsOutdated(ctx context.Context) (bool, error)

	// GetLocalNodeAddress returns the address of the node this method is invoked on.
	GetLocalNodeAddress(ctx context.Context) (string, error)

	// SetNodeHeartbeat updates the heartbeat column of the node with the given address.
	SetNodeHeartbeat(ctx context.Context, address string, heartbeatTime time.Time) error

	// GetPendingNodeByAddress returns the pending node with the given network address.
	GetPendingNodeByAddress(ctx context.Context, address string) (NodeInfo, error)

	// GetPendingNodeByName returns the pending node with the given name.
	GetPendingNodeByName(ctx context.Context, name string) (NodeInfo, error)

	// BootstrapNode sets the name and address of the first cluster member, with id: 1.
	BootstrapNode(ctx context.Context, serverName string, clusterAddress string) error

	// CreateNodeWithArch adds a node to the current list of members that are part of the
	// cluster. The node's architecture should be the architecture of the machine the
	// method is being run on. It returns the ID of the newly inserted row.
	CreateNodeWithArch(ctx context.Context, name, address string, arch int) (int64, error)

	// SetNodePendingFlag toggles the pending flag for the node. A node is pending when
	// it's been accepted in the cluster, but has not yet actually joined it.
	SetNodePendingFlag(ctx context.Context, nodeID int64, flag bool) error

	// EnsureCertificateTrusted adds the serverCert to the DB trusted certificates store using the serverName.
	EnsureCertificateIsTrusted(ctx context.Context, serverName string, serverCert *x509.Certificate) error

	// DeleteCertificatesByMemberName removes the certificate associated with a cluster member.
	DeleteCertificatesByMemberName(ctx context.Context, name string) error

	// ResolveOfflineMemberWarning resolves offline member warnings for the given nodeID and serverName.
	ResolveOfflineMemberWarning(ctx context.Context, nodeID int64, serverName string) error

	// EmitOfflineMemberWarning emits an offline member warning for the given nodeID and serverName.
	EmitOfflineMemberWarning(ctx context.Context, nodeID int64, serverName string, msg string) error

	// ResolveTimeSkewWarning resolves time skew warnings for the given serverName.
	ResolveTimeSkewWarning(ctx context.Context, serverName string) error

	// EmitTimeSkewWarning emits a time skew warning for the given serverName.
	EmitTimeSkewWarning(ctx context.Context, serverName string, msg string) error

	// EnterExclusive should block the opening of any transactions after called.
	// The Transactor's OnTxStart should handle clearing this state in its returned cleanup func.
	EnterExclusive() error
}

// ClusterResourceUpdater represents the implementation for updating external cluster resources on join.
type ClusterResourceUpdater[T any] interface {
	GetLocalResources(ctx context.Context) (T, error)
	UpdateClusterResources(ctx context.Context, node NodeInfo, t T) error
}
