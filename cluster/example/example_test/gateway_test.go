//go:build !nosqlite3

package example_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/example"
	"github.com/cowsql/go-cowsql/cluster/gateway"
	"github.com/cowsql/go-cowsql/cluster/membership"
	"github.com/cowsql/go-cowsql/cluster/options"
	"github.com/cowsql/go-cowsql/cluster/state"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
	"github.com/cowsql/go-cowsql/driver"
)

func trustedCerts() map[string]x509.Certificate {
	return nil
}

// Basic creation and shutdown. By default, the gateway runs an in-memory gRPC
// server.
func TestGateway_Single(t *testing.T) {
	node, cleanup := NewTestNode(t, "")
	defer cleanup()

	cert := TestingKeyPair(t)

	s := &example.State{}

	serverCertFunc := func() cowsqltls.CertInfo { return cert }

	gateway := newGateway(t, node, cert, s, serverCertFunc, nil)
	defer func() { _ = gateway.ShutdownServer() }()

	authorizer := example.Authorizer(gateway.NetworkCert, gateway.ServerCert, trustedCerts)
	handlerFuncs := gateway.HandlerFuncs(authorizer)
	require.Len(t, handlerFuncs, 1)

	for endpoint, f := range handlerFuncs {
		c, err := x509.ParseCertificate(cert.KeyPair().Certificate[0])
		require.NoError(t, err)

		w := httptest.NewRecorder()
		r := &http.Request{}
		r.Header = http.Header{}
		r.Header.Set("X-Dqlite-Version", "1")
		r.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{c},
		}

		f(w, r)
		require.Equal(t, 404, w.Code, endpoint)
	}

	dial := gateway.DialFunc()
	netConn, err := dial(context.Background(), "")
	require.NoError(t, err)
	require.NotNil(t, netConn)
	require.NoError(t, netConn.Close())

	leader, err := gateway.LeaderAddress()
	require.Empty(t, leader)
	require.EqualError(t, err, membership.ErrNodeIsNotClustered.Error())

	cowsqlDriver, err := driver.New(
		gateway.NodeStore(),
		driver.WithDialFunc(gateway.DialFunc()),
	)
	require.NoError(t, err)

	conn, err := cowsqlDriver.Open("test.db")
	require.NoError(t, err)

	require.NoError(t, conn.Close())
}

// If there's a network address configured, we expose the cowsql endpoint with
// an HTTP handler.
func TestGateway_SingleWithNetworkAddress(t *testing.T) {
	cert := TestingKeyPair(t)
	mux := http.NewServeMux()

	server := newServer(cert, mux)
	defer server.Close()

	address := server.Listener.Addr().String()

	node, cleanup := NewTestNode(t, address)
	defer cleanup()

	setRaftRole(t, node, address)

	s := &example.State{}

	serverCertFunc := func() cowsqltls.CertInfo { return cert }

	gateway := newGateway(t, node, cert, s, serverCertFunc, nil)
	defer func() { _ = gateway.ShutdownServer() }()

	authorizer := example.Authorizer(gateway.NetworkCert, gateway.ServerCert, trustedCerts)
	for path, handler := range gateway.HandlerFuncs(authorizer) {
		mux.HandleFunc(path, handler)
	}

	cowsqlDriver, err := driver.New(
		gateway.NodeStore(),
		driver.WithDialFunc(gateway.DialFunc()),
	)
	require.NoError(t, err)

	conn, err := cowsqlDriver.Open("test.db")
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	leader, err := gateway.LeaderAddress()
	require.NoError(t, err)
	require.Equal(t, address, leader)
}

// When networked, the grpc and raft endpoints requires the cluster
// certificate.
func TestGateway_NetworkAuth(t *testing.T) {
	cert := TestingKeyPair(t)
	mux := http.NewServeMux()

	server := newServer(cert, mux)
	defer server.Close()

	address := server.Listener.Addr().String()

	node, cleanup := NewTestNode(t, address)
	defer cleanup()

	setRaftRole(t, node, address)

	s := example.NewState(node, server.Listener, func(ctx context.Context) {})

	serverCertFunc := func() cowsqltls.CertInfo { return cert }

	gateway := newGateway(t, node, cert, s, serverCertFunc, nil)
	defer func() { _ = gateway.ShutdownServer() }()

	authorizer := example.Authorizer(gateway.NetworkCert, gateway.ServerCert, trustedCerts)
	for path, handler := range gateway.HandlerFuncs(authorizer) {
		mux.HandleFunc(path, handler)
	}

	// Make a request using a certificate different than the cluster one.
	certAlt := TestingAltKeyPair(t)
	config, err := cowsqltls.ClientConfig(certAlt, certAlt, false)
	config.InsecureSkipVerify = true // Skip client-side verification

	require.NoError(t, err)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: config}}

	for path := range gateway.HandlerFuncs(authorizer) {
		url := fmt.Sprintf("https://%s%s", address, path)
		response, err := client.Head(url) //nolint:noctx
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.Equal(t, http.StatusForbidden, response.StatusCode)
	}
}

// RaftNodes returns all nodes of the.
func TestGateway_RaftNodesNotLeader(t *testing.T) {
	cert := TestingKeyPair(t)
	mux := http.NewServeMux()

	server := newServer(cert, mux)
	defer server.Close()

	address := server.Listener.Addr().String()

	node, cleanup := NewTestNode(t, address)
	defer cleanup()

	setRaftRole(t, node, address)

	s := &example.State{}
	serverCertFunc := func() cowsqltls.CertInfo { return cert }

	gateway := newGateway(t, node, cert, s, serverCertFunc, nil)
	defer func() { _ = gateway.ShutdownServer() }()

	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	require.NoError(t, err)

	require.Len(t, nodes, 1)
	require.Equal(t, uint64(1), nodes[0].ID)
	require.Equal(t, nodes[0].Address, address)
}

// Create a new test Gateway with the given parameters, and ensure no error happens.
func newGateway(t *testing.T, node db.Node, networkCert cowsqltls.CertInfo, s state.State, serverCertFunc func() cowsqltls.CertInfo, clusterDB db.Cluster, opts ...options.Option) cluster.Gateway {
	t.Helper()
	require.NoError(t, os.Mkdir(node.GlobalDatabaseDir(), 0o755))

	allOpts := []options.Option{
		options.Latency(0.2),
		options.LogLevel("TRACE"),
	}

	allOpts = append(allOpts, opts...)

	gateway, err := gateway.NewGateway(t.Context(), node, networkCert, serverCertFunc, s, allOpts...)
	require.NoError(t, err)

	if clusterDB != nil {
		gateway.SetClusterDB(clusterDB)
	}

	return gateway
}
