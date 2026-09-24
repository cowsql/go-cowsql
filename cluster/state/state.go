package state

import (
	"context"

	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

// State represents the implementation for non-database related state information.
type State interface {
	// NewClusterCertificate should generate a new TLS certificate and return its details.
	NewClusterCertificate() (tls.CertInfo, error)

	// SetClusterCertificate should persist the given certificate details.
	SetClusterCertificate(i tls.CertInfo) (tls.CertInfo, error)

	// ClusterAddress returns the address used for intra-cluster communication.
	ClusterAddress() string

	// UpdateAuthorizer triggers an update to the handler func authorizer.
	UpdateAuthorizer(ctx context.Context) error

	// HasListener returns whether any network listeners are active.
	HasListener(ctx context.Context) error

	// OnHeartbeatNotification updates events that depend on heartbeats.
	OnHeartbeatNotification(members map[int64]db.HeartbeatMember)
}
