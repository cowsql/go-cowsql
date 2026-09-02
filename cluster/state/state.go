package state

import (
	"context"

	"github.com/cowsql/go-cowsql/cluster/db"
	incustls "github.com/lxc/incus/v7/shared/tls"
)

// State represents the implementation for non-database related state information.
type State interface {
	// CertificateDir returns the containing directory for certificates.
	CertificateDir() string

	// ClusterAddress returns the address used for intra-cluster communication.
	ClusterAddress() string

	// UpdateCertificateCache updates the in-memory cache of cluster member server certificates.
	UpdateCertificateCache(ctx context.Context) error

	// IsListening returns whether any network listeners are active.
	IsListening(ctx context.Context) error

	// UpdateListenerCertificate updates the certificate used by the network listener.
	UpdateListenerCertificate(ctx context.Context, cert *incustls.CertInfo) error

	// DeleteDatabase clears all database state on the filesystem.
	DeleteDatabase(ctx context.Context) error

	// UpdateEventListeners updates events that depend on heartbeats.
	UpdateEventListeners(members map[int64]db.HeartbeatMember)
}
