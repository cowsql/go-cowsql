//go:build !nosqlite3

package example_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/example"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/membership"
	"github.com/cowsql/go-cowsql/cluster/options"
	"github.com/cowsql/go-cowsql/cluster/state"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
	"github.com/cowsql/go-cowsql/driver"
)

func heartbeatTask(ctx context.Context, gateway cluster.Gateway) {
	if gateway.HeartbeatCancelFunc() == nil {
		ch := make(chan struct{})

		go func() {
			gateway.Heartbeat(ctx, heartbeat.HeartbeatNormal)
			close(ch)
		}()

		select {
		case <-ch:
		case <-ctx.Done():
		}
	}
}

// After a heartbeat request is completed, the leader updates the heartbeat
// timestamp column, and the serving node updates its cache of raft nodes.
func TestHeartbeat(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	f.Bootstrap()

	f.Grow()
	f.Grow()

	time.Sleep(1 * time.Second) // Wait for join notification triggered heartbeats to complete.

	leader := f.Leader()

	// Artificially mark all nodes as down
	err := transaction.ForceTx(t.Context(), leader.Cluster(), func(ctx context.Context, tx transaction.TX) error {
		members, err := leader.Cluster().GetNodes(ctx)
		require.NoError(t, err)

		for _, member := range members {
			err := leader.Cluster().SetNodeHeartbeat(ctx, member.Address, time.Now().Add(-time.Minute))
			require.NoError(t, err)
		}

		return nil
	})
	require.NoError(t, err)

	// Perform the heartbeat requests.
	leader.SetClusterDB(leader.Cluster())
	heartbeatTask(t.Context(), leader)

	// The heartbeat timestamps of all nodes got updated
	err = transaction.ForceTx(t.Context(), leader.Cluster(), func(ctx context.Context, tx transaction.TX) error {
		members, err := leader.Cluster().GetNodes(ctx)
		require.NoError(t, err)

		offlineThreshold, err := leader.Cluster().GetNodeOfflineThreshold(ctx)
		require.NoError(t, err)

		for _, member := range members {
			require.False(t, member.IsOffline(offlineThreshold))
		}

		return nil
	})
	require.NoError(t, err)
}

// Helper for testing heartbeat-related code.
type heartbeatFixture struct {
	t        *testing.T
	gateways map[int]cluster.Gateway              // node index to gateway
	states   map[cluster.Gateway]state.State      // gateway to its state handle
	servers  map[cluster.Gateway]*httptest.Server // gateway to its HTTP server
	cleanups []func()
}

// Bootstrap the first node of the cluster.
func (f *heartbeatFixture) Bootstrap() cluster.Gateway {
	f.t.Logf("create bootstrap node for test cluster")
	_, gateway, _ := f.node()

	err := membership.Bootstrap(gateway, "buzz")
	require.NoError(f.t, err)

	return gateway
}

// Grow adds a new node to the cluster.
func (f *heartbeatFixture) Grow() cluster.Gateway {
	// Figure out the current leader
	f.t.Logf("adding another node to the test cluster")
	target := f.Leader()

	_, gateway, address := f.node()
	name := address

	joinerCert, err := gateway.ServerCert().PublicKeyX509()
	require.NoError(f.t, err)

	nodes, err := membership.Accept(target, joinerCert, name, address, 1, 1, 1)
	require.NoError(f.t, err)

	err = membership.Join[struct{}](gateway, target.NetworkCert().KeyPair(), name, nodes)
	require.NoError(f.t, err)

	return gateway
}

// Return the leader gateway in the cluster.
func (f *heartbeatFixture) Leader() cluster.Gateway {
	timeout := time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		for _, gateway := range f.gateways {
			isLeader, err := gateway.IsLeader(context.TODO())
			if err != nil {
				f.t.Errorf("failed to check leadership: %v", err)
			}

			if isLeader {
				return gateway
			}
		}

		select {
		case <-ctx.Done():
			f.t.Errorf("no leader was elected within %s", timeout)
		default:
		}

		// Wait a bit for election to take place
		time.Sleep(10 * time.Millisecond)
	}
}

// Return a follower gateway in the cluster.
func (f *heartbeatFixture) Follower() cluster.Gateway {
	timeout := time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		for _, gateway := range f.gateways {
			isLeader, err := gateway.IsLeader(context.TODO())
			if err != nil {
				f.t.Errorf("failed to check leadership: %v", err)
			}

			if !isLeader {
				return gateway
			}
		}

		select {
		case <-ctx.Done():
			f.t.Errorf("no node running as follower")
		default:
		}

		// Wait a bit for election to take place
		time.Sleep(10 * time.Millisecond)
	}
}

// Return the cluster index of the given gateway.
func (f *heartbeatFixture) Index(gateway cluster.Gateway) int {
	for i := range f.gateways {
		if f.gateways[i] == gateway {
			return i
		}
	}

	return -1
}

// Return the state associated with the given gateway.
func (f *heartbeatFixture) State(gateway cluster.Gateway) state.State {
	return f.states[gateway]
}

// Return the HTTP server associated with the given gateway.
func (f *heartbeatFixture) Server(gateway cluster.Gateway) *httptest.Server {
	return f.servers[gateway]
}

// Creates a new node, without either bootstrapping or joining it.
//
// Return the associated gateway and network address.
func (f *heartbeatFixture) node(opts ...options.Option) (state.State, cluster.Gateway, string) {
	if f.gateways == nil {
		f.gateways = make(map[int]cluster.Gateway)
		f.states = make(map[cluster.Gateway]state.State)
		f.servers = make(map[cluster.Gateway]*httptest.Server)
	}

	serverCert := TestingKeyPair(f.t)
	serverCertFunc := func() cowsqltls.CertInfo { return serverCert }
	mux := http.NewServeMux()
	server := newServer(serverCert, mux)
	address := server.Listener.Addr().String()
	node, nodeCleanup := NewTestNode(f.t, address)
	f.cleanups = append(f.cleanups, nodeCleanup)
	s := example.NewState(node, server.Listener, func(ctx context.Context) {})

	gateway := newGateway(f.t, node, serverCert, s, serverCertFunc, nil, opts...)
	f.cleanups = append(f.cleanups, func() { _ = gateway.ShutdownServer() })

	for path, handler := range gateway.HandlerFuncs(example.Authorizer(gateway.NetworkCert, gateway.ServerCert, trustedCerts)) {
		mux.HandleFunc(path, handler)
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(address))
	})

	store := gateway.NodeStore()
	dial := gateway.DialFunc()
	drv, err := driver.New(store, driver.WithDialFunc(dial))
	require.NoError(f.t, err)
	clusterDB, err := example.OpenClusterDB(f.t.Context(), drv, f.t.TempDir(), 5*time.Second)
	require.NoError(f.t, err)
	f.cleanups = append(f.cleanups, func() { _ = clusterDB.Close() })
	cluster, err := example.NewCluster(f.t.Context(), clusterDB)
	require.NoError(f.t, err)
	gateway.SetClusterDB(cluster)

	f.gateways[len(f.gateways)] = gateway
	f.states[gateway] = s
	f.servers[gateway] = server

	return s, gateway, address
}

func (f *heartbeatFixture) Cleanup() {
	// Run the cleanups in reverse order
	for _, v := range slices.Backward(f.cleanups) {
		v()
	}

	for _, server := range f.servers {
		server.Close()
	}
}

// NewTestNode creates a new Node for testing purposes, along with a function
// that can be used to clean it up when done.
func NewTestNode(t *testing.T, clusterAddress string) (db.Node, func()) {
	t.Helper()
	dir := t.TempDir()

	db, err := example.OpenLocalDB(dir)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, db.Close())
		require.NoError(t, os.RemoveAll(dir))
	}

	node, err := example.NewNode(t.Context(), db, clusterAddress, dir)
	require.NoError(t, err)

	return node, cleanup
}
