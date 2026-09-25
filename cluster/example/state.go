//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"

	"github.com/cowsql/go-cowsql/cluster/db"
	tlsutil "github.com/cowsql/go-cowsql/cluster/example/tls"
	"github.com/cowsql/go-cowsql/cluster/state"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

// State represents general information about the server.
type State struct {
	db       db.Node
	listener net.Listener

	refreshCertFunc func(ctx context.Context)
}

// NewState returns a new state object.
func NewState(db db.Node, listener net.Listener, refreshCertFunc func(context.Context)) *State {
	return &State{
		db:              db,
		listener:        listener,
		refreshCertFunc: refreshCertFunc,
	}
}

// ClusterAddress implements [state.State].
// Returns the current cluster address.
func (s *State) ClusterAddress() string {
	address, _ := s.db.GetClusterAddress(context.TODO())
	return address
}

// HasListener implements [state.State].
// Return whether the server listener has been set up.
func (s *State) HasListener(ctx context.Context) error {
	if s.listener != nil {
		return nil
	}

	return errors.New("Network connectivity has not been set up")
}

// NewClusterCertificate implements [state.State].
// Generate a tls.CertInfo to use as the cluster certificate.
func (s *State) NewClusterCertificate() (tls.CertInfo, error) {
	cert, err := tlsutil.KeyPairAndCA(filepath.Dir(filepath.Dir(s.db.GlobalDatabaseDir())), "cluster")
	if err != nil {
		return nil, err
	}

	tlsListener, ok := s.listener.(*tlsutil.FancyTLSListener)
	if ok {
		tlsListener.Config(cert)
	}

	return cert, nil
}

// SetClusterCertificate implements [state.State].
// set the cluster certificate with the given details returned from a join request.
func (s *State) SetClusterCertificate(i tls.CertInfo) (tls.CertInfo, error) {
	certDir := filepath.Dir(filepath.Dir(s.db.GlobalDatabaseDir()))

	err := os.WriteFile(filepath.Join(certDir, "cluster.crt"), i.PublicKey(), 0o644)
	if err != nil {
		return nil, err
	}

	err = os.WriteFile(filepath.Join(certDir, "cluster.key"), i.PrivateKey(), 0o600)
	if err != nil {
		return nil, err
	}

	cert, err := tlsutil.KeyPairAndCA(certDir, "cluster")
	if err != nil {
		return nil, err
	}

	tlsListener, ok := s.listener.(*tlsutil.FancyTLSListener)
	if ok {
		tlsListener.Config(cert)
	}

	return cert, nil
}

// UpdateAuthenticator implements [state.State].
// Update the cache of cluster member server certificates used for TLS authentication of the cowsql connection API.
func (s *State) UpdateAuthenticator(ctx context.Context) error {
	if s.refreshCertFunc != nil {
		s.refreshCertFunc(ctx)
	}

	return nil
}

// OnHeartbeatNotification implements [state.State].
// Trigger any required actions upon an out-of-interval heartbeat message, usually triggered by cluster role changes.
func (s *State) OnHeartbeatNotification(members map[int64]db.HeartbeatMember) {}

var _ state.State = &State{}
