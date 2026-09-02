//go:build !nosqlite3

package gateway

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	incustls "github.com/lxc/incus/v7/shared/tls"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/api"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/internal/response"
	"github.com/cowsql/go-cowsql/cluster/membership"
	"github.com/cowsql/go-cowsql/cluster/options"
	"github.com/cowsql/go-cowsql/cluster/state"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

func NewGateway(shutdownCtx context.Context, db db.Node, networkCert *incustls.CertInfo, serverCert func() *incustls.CertInfo, state state.State, opts ...options.Option) (cluster.Gateway, error) {
	ctx, cancel := context.WithCancel(context.TODO())

	o := options.NewOptions()
	for _, option := range opts {
		option(o)
	}

	g := &gateway{
		shutdownCtx: shutdownCtx,
		db:          db,
		networkCert: networkCert,
		serverCert:  serverCert,
		options:     o,
		ctx:         ctx,
		cancel:      cancel,
		upgradeCh:   make(chan struct{}),
		acceptCh:    make(chan net.Conn),
		store:       &cowsqlNodeStore{},
		state:       state,
	}

	err := g.init(false)
	if err != nil {
		return nil, err
	}

	return g, nil
}

func (g *gateway) State() state.State {
	return g.state
}

func (g *gateway) Node() db.Node {
	return g.db
}

func (g *gateway) Cluster() db.Cluster {
	return g.cluster
}

func (g *gateway) Initialize(bootstrap bool) error {
	return g.init(bootstrap)
}

func (g *gateway) UserConfig() *options.Options {
	return g.options
}

func (g *gateway) HeartbeatOfflineThreshold() time.Duration {
	return g.heartbeatOfflineThreshold
}

func (g *gateway) SetHeartbeatOfflineThreshold(t time.Duration) {
	g.heartbeatOfflineThreshold = t
}

func (g *gateway) RaftDial() client.DialFunc {
	return g.raftDial()
}

func (g *gateway) Standalone() bool {
	return g.memoryDial != nil
}

func (g *gateway) NetworkCert() *incustls.CertInfo {
	return g.networkCert
}

func (g *gateway) RaftNode() *db.RaftNode {
	return g.info
}

func (g *gateway) SetRaftNode(n *db.RaftNode) {
	g.info = n
}

func (g *gateway) RaftClient(ctx context.Context) (*client.Client, error) {
	return client.New(ctx, g.bindAddress)
}

func (g *gateway) SetClusterDB(c db.Cluster) {
	g.cluster = c
}

func (g *gateway) SetHeartbeatNodeHook(f heartbeat.Hook) {
	g.heartbeatNodeHook = f
}

func (g *gateway) AwaitHeartbeat() {
	// Wait for heartbeat to finish and then release.
	// Ignore staticcheck "SA2001: empty critical section" because we want to wait for the lock.
	g.heartbeatLock.Lock()
	g.heartbeatLock.Unlock() //nolint:staticcheck
}

func (g *gateway) ServerCert() *incustls.CertInfo {
	return g.serverCert()
}

// HandlerFuncs returns the HTTP handlers that should be added to the REST API
// endpoint in order to handle database-related requests.
//
// There are two handlers, one for the /internal/raft endpoint and the other
// for /internal/db, which handle respectively raft and gRPC-SQL requests.
//
// These handlers might return 404, either because this server is a
// non-clustered member not available over the network or because it is not a
// database node part of the cowsql cluster.
func (g *gateway) HandlerFuncs(trustedCerts func() (map[string]x509.Certificate, error)) map[string]http.HandlerFunc {
	database := func(w http.ResponseWriter, r *http.Request) {
		g.lock.RLock()

		certs, err := trustedCerts()
		if err != nil {
			g.lock.RUnlock()
			http.Error(w, "403 invalid client certificate", http.StatusForbidden)
			return
		}

		if !tls.CheckCert(r, g.networkCert, g.serverCert(), certs) {
			g.lock.RUnlock()
			http.Error(w, "403 invalid client certificate", http.StatusForbidden)
			return
		}

		g.lock.RUnlock()

		// Compare the cowsql version of the connecting client
		// with our own one.
		versionHeader := r.Header.Get("X-Dqlite-Version")
		if versionHeader == "" {
			// No version header means an old pre cowsql 1.0 client.
			versionHeader = "0"
		}

		version, err := strconv.Atoi(versionHeader)
		if err != nil {
			http.Error(w, "400 invalid cowsql version", http.StatusBadRequest)
			return
		}

		if version != api.COWSQLVersion {
			if version > api.COWSQLVersion {
				g.lock.Lock()
				if !g.upgradeTriggered {
					err = membership.TriggerUpdate(g)
					if err == nil {
						g.upgradeTriggered = true
					}
				}
				g.lock.Unlock()
				http.Error(w, "503 unsupported cowsql version", http.StatusServiceUnavailable)
			} else {
				http.Error(w, "426 cowsql version too old ", http.StatusUpgradeRequired)
			}

			return
		}

		// Handle heartbeats (these normally come from leader, but can come from joining nodes too).
		if r.Method == "PUT" {
			if g.shutdownCtx.Err() != nil {
				slog.Warn("Rejecting heartbeat request as shutting down")
				http.Error(w, "503 Shutting down", http.StatusServiceUnavailable)
				return
			}

			var heartbeatData heartbeat.APIHeartbeat
			err := json.NewDecoder(r.Body).Decode(&heartbeatData)
			if err != nil {
				slog.Error("Failed decoding heartbeat", "err", err)
				http.Error(w, "400 Failed decoding heartbeat", http.StatusBadRequest)
				return
			}

			g.lock.RLock()
			isLeader, err := g.IsLeader(context.TODO())
			g.lock.RUnlock()
			if err != nil {
				slog.Error("Failed checking if leader", "err", err)
				http.Error(w, "500 Failed checking if leader", http.StatusInternalServerError)
				return
			}

			g.heartbeatHandler(w, r, isLeader, &heartbeatData)

			return
		}

		// Handle database upgrade notifications.
		if r.Method == "PATCH" {
			select {
			case g.upgradeCh <- struct{}{}:
			default:
			}

			return
		}

		// From here on we require that this node is part of the raft
		// cluster.
		g.lock.RLock()
		if g.server == nil || g.memoryDial != nil {
			g.lock.RUnlock()
			http.NotFound(w, r)
			return
		}

		g.lock.RUnlock()

		// NOTE: this is kept for backward compatibility when upgrading
		// a cluster with version <= 4.2.
		//
		// Once all nodes are on >= 4.3 this code is effectively
		// unused.
		if r.Method == "HEAD" {
			g.lock.RLock()
			defer g.lock.RUnlock()
			// We can safely know about current leader only if we are a voter.
			if g.info.Role != db.RaftVoter {
				http.NotFound(w, r)
				return
			}

			cowsqlClient, err := g.RaftClient(context.TODO())
			if err != nil {
				http.Error(w, "500 failed to get cowsql client", http.StatusInternalServerError)
				return
			}

			defer func() {
				err := cowsqlClient.Close()
				if err != nil {
					slog.Warn("Failed to close client", "err", err)
				}
			}()

			ctx, cancel := context.WithTimeout(g.ctx, 3*time.Second)
			defer cancel()
			leader, err := cowsqlClient.Leader(ctx)
			if err != nil {
				http.Error(w, "500 failed to get leader address", http.StatusInternalServerError)
				return
			}

			if leader == nil || leader.ID != g.info.ID {
				http.Error(w, "503 not leader", http.StatusServiceUnavailable)
				return
			}

			return
		}

		// Handle leader address requests.
		if r.Method == "GET" {
			leader, err := g.LeaderAddress()
			if err != nil {
				http.Error(w, "500 no elected leader", http.StatusInternalServerError)
				return
			}

			_ = writeJSON(w, map[string]string{"leader": leader}, nil)
			return
		}

		if r.Header.Get("Upgrade") != "dqlite" {
			http.Error(w, "Missing or invalid upgrade header", http.StatusBadRequest)
			return
		}

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Webserver doesn't support hijacking", http.StatusInternalServerError)

			return
		}

		conn, _, err := hijacker.Hijack()
		if err != nil {
			http.Error(w, fmt.Errorf("Failed to hijack connection: %w", err).Error(), http.StatusInternalServerError)

			return
		}

		err = response.Upgrade(conn, "dqlite")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			_ = conn.Close()

			return
		}

		g.acceptCh <- conn
	}

	return map[string]http.HandlerFunc{
		g.UserConfig().DatabaseEndpoint(): database,
	}
}

// WaitUpgradeNotification waits for a notification from another node that all
// nodes in the cluster should now have been upgraded and have matching schema
// and API versions.
func (g *gateway) WaitUpgradeNotification() {
	select {
	case <-g.upgradeCh:
	case <-time.After(time.Minute):
	}
}

// IsCowsqlNode returns true if this gateway is running a cowsql node.
func (g *gateway) IsCowsqlNode() bool {
	g.lock.RLock()
	defer g.lock.RUnlock()

	if g.info != nil {
		if g.server == nil {
			panic("gateway has node identity but no cowsql server")
		}

		return true
	}

	if g.server != nil {
		panic("gateway cowsql server but no node identity")
	}

	return true
}

// DialFunc returns a dial function that can be used to connect to one of the
// cowsql nodes.
func (g *gateway) DialFunc() client.DialFunc {
	return func(ctx context.Context, address string) (net.Conn, error) {
		g.lock.RLock()
		defer g.lock.RUnlock()

		// Memory connection.
		if g.memoryDial != nil {
			return g.memoryDial(ctx, address)
		}

		conn, err := cowsqlNetworkDial(ctx, "dqlite", address, g)
		if err != nil {
			return nil, err
		}

		// We successfully established a connection with the leader. Maybe the
		// leader is ourselves, and we were recently elected. In that case
		// trigger a full heartbeat now: it will be a no-op if we aren't
		// actually leaders.
		go g.Heartbeat(g.ctx, heartbeat.HeartbeatInitial)

		return conn, nil
	}
}

// Context returns a cancellation context to pass to cowsql.NewDriver as
// option.
//
// This context gets cancelled by Gateway.Kill() and at that point any
// connection failure won't be retried.
func (g *gateway) Context() context.Context {
	return g.ctx
}

// NodeStore returns a cowsql server store that can be used to lookup the
// addresses of known database nodes.
func (g *gateway) NodeStore() client.NodeStore {
	return g.store
}

// Kill is an API that the daemon calls before it actually shuts down and calls
// Shutdown(). It will abort any ongoing or new attempt to establish a SQL gRPC
// connection with the dialer (typically for running some pre-shutdown
// queries).
func (g *gateway) Kill() {
	slog.Debug("Cancel ongoing or future gRPC connection attempts")
	g.cancel()
}

// TransferLeadership attempts to transfer leadership to another node.
func (g *gateway) TransferLeadership(ctx context.Context) error {
	cowsqlClient, err := g.RaftClient(ctx)
	if err != nil {
		return err
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client", "err", err)
		}
	}()

	// Try to find a voter that is also online.
	servers, err := cowsqlClient.Cluster(ctx)
	if err != nil {
		return err
	}

	var id uint64
	for _, server := range servers {
		if server.ID == g.info.ID || server.Role != db.RaftVoter {
			continue
		}

		address, err := g.nodeAddress(context.TODO(), server.Address)
		if err != nil {
			return err
		}

		if !cluster.HasConnectivity(g.networkCert, g.serverCert(), address) {
			continue
		}

		id = server.ID
		break
	}

	if id == 0 {
		return membership.ErrNoOnlineVoter
	}

	return cowsqlClient.Transfer(ctx, id)
}

// DemoteOfflineNode force demoting an offline node.
func (g *gateway) DemoteOfflineNode(raftID uint64) error {
	cli, err := g.RaftClient(context.TODO())
	if err != nil {
		return fmt.Errorf("Connect to local cowsql node: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cli.Assign(ctx, raftID, db.RaftSpare)
	if err != nil {
		return err
	}

	return nil
}

// Shutdown this gateway, stopping the gRPC server and possibly the raft factory.
func (g *gateway) Shutdown() error {
	slog.Debug("Stop database gateway")

	var err error
	if g.server != nil {
		if g.info.Role == db.RaftVoter {
			g.Sync()
		}

		err = g.server.Close()
		close(g.stopCh)

		// Unset the memory dial, since Shutdown() is also called for
		// switching between in-memory and network mode.
		g.lock.Lock()
		g.memoryDial = nil
		g.lock.Unlock()
	}

	return err
}

// Sync dumps the content of the database to disk. This is useful for
// inspection purposes, and it's also needed by the activateifneeded command so
// it can inspect the database in order to decide whether to activate the
// daemon or not.
func (g *gateway) Sync() {
	g.lock.RLock()
	defer g.lock.RUnlock()

	if g.server == nil || g.info.Role != db.RaftVoter {
		return
	}

	cowsqlClient, err := g.RaftClient(context.TODO())
	if err != nil {
		slog.Warn("Failed to get client", "err", err)
		return
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client", "err", err)
		}
	}()

	files, err := cowsqlClient.Dump(context.Background(), "db.bin")
	if err != nil {
		// Just log a warning, since this is not fatal.
		slog.Warn("Failed get database dump", "err", err)
		return
	}

	dir := filepath.Join(g.db.Dir(), "global")
	for _, file := range files {
		path := filepath.Join(dir, file.Name)
		err := os.WriteFile(path, file.Data, 0o600)
		if err != nil {
			slog.Warn("Failed to dump database file", "file", file.Name, "err", err)
		}
	}
}

// Reset the gateway, shutting it down.
//
// This is used when disabling clustering on a node.
func (g *gateway) Reset(networkCert *incustls.CertInfo) error {
	err := g.Shutdown()
	if err != nil {
		return err
	}

	err = os.RemoveAll(filepath.Join(g.db.Dir(), "global"))
	if err != nil {
		return err
	}

	err = transaction.Do(context.TODO(), g.Node(), func(ctx context.Context) error {
		return g.db.ReplaceRaftNodes(ctx, nil)
	})
	if err != nil {
		return err
	}

	g.networkCert = networkCert

	return nil
}

// HearbeatCancelFunc returns the function that can be used to cancel an ongoing heartbeat.
// Returns nil if no ongoing heartbeat.
func (g *gateway) HearbeatCancelFunc() func() {
	g.heartbeatCancelLock.Lock()
	defer g.heartbeatCancelLock.Unlock()
	return g.heartbeatCancel
}

// NetworkUpdateCert sets a new network certificate for the gateway
// Use with Endpoints.NetworkUpdateCert() to fully update the API endpoint.
func (g *gateway) NetworkUpdateCert(cert *incustls.CertInfo) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.networkCert = cert
}

// WaitLeadership waits for the raft node to become leader.
func (g *gateway) WaitLeadership() error {
	n := 80
	sleep := 250 * time.Millisecond
	for range n {
		g.lock.RLock()
		isLeader, err := g.IsLeader(context.TODO())
		if err != nil {
			g.lock.RUnlock()
			return err
		}

		if isLeader {
			g.lock.RUnlock()
			return nil
		}

		g.lock.RUnlock()

		time.Sleep(sleep)
	}

	return fmt.Errorf("RAFT node did not self-elect within %s", time.Duration(n)*sleep)
}

// LeaderAddress returns the address of the current raft leader.
func (g *gateway) LeaderAddress() (string, error) {
	g.lock.RLock()
	defer g.lock.RUnlock()

	// If we aren't clustered, return an error.
	if g.memoryDial != nil {
		return "", membership.ErrNodeIsNotClustered
	}

	// If this is a voter node, return the address of the current leader, or wait a bit until one is elected.
	if g.server != nil && g.info.Role == db.RaftVoter {
		ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
		defer cancel()

		for {
			cowsqlClient, err := g.RaftClient(context.TODO())
			if err != nil {
				return "", fmt.Errorf("Failed to get cowsql client: %w", err)
			}

			leader, err := cowsqlClient.Leader(ctx)
			if err != nil {
				_ = cowsqlClient.Close()
				return "", fmt.Errorf("Failed to get leader address: %w", err)
			}

			if leader != nil && leader.Address != "" {
				_ = cowsqlClient.Close()
				return leader.Address, nil
			}

			_ = cowsqlClient.Close()

			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
	}

	var addresses []string
	err := transaction.Do(context.TODO(), g.Node(), func(ctx context.Context) error {
		nodes, err := g.db.GetRaftNodes(ctx)
		if err != nil {
			return err
		}

		for _, node := range nodes {
			if node.Role != db.RaftVoter {
				continue
			}

			addresses = append(addresses, node.Address)
		}

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("Failed to fetch raft nodes addresses: %w", err)
	}

	if len(addresses) == 0 {
		// This should never happen because the raft_nodes table should
		// be never empty for a clustered node, but check it for good
		// measure.
		return "", errors.New("No raft node known")
	}

	transport, cleanup, err := tls.Transport(g.networkCert, g.serverCert())
	if err != nil {
		return "", err
	}

	defer cleanup()

	for _, address := range addresses {
		timeout := 2 * time.Second
		httpClient := &http.Client{
			Transport: transport,
			Timeout:   timeout,
		}

		requestURL := fmt.Sprintf("https://%s%s", address, g.UserConfig().DatabaseEndpoint())
		req, err := http.NewRequest("GET", requestURL, nil)
		if err != nil {
			return "", err
		}

		api.SetCOWSQLVersionHeader(req)

		// Use 1s later timeout to give HTTP client chance timeout with
		// more useful info.
		ctx, cancel := context.WithTimeout(g.ctx, timeout+time.Second)
		req = req.WithContext(ctx)
		resp, err := httpClient.Do(req)
		if err != nil {
			cancel()
			slog.Debug("Failed to fetch leader address", "address", address)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			cancel()
			slog.Debug("Request for leader address failed", "address", address)
			continue
		}

		info := map[string]string{}
		err = json.NewDecoder(resp.Body).Decode(&info)
		if err != nil {
			cancel()
			slog.Debug("Failed to parse leader address", "address", address)
			continue
		}

		leader := info["leader"]
		if leader == "" {
			cancel()
			slog.Debug("Raft node returned no leader address", "address", address)
			continue
		}

		cancel()
		return leader, nil
	}

	return "", errors.New("RAFT cluster is unavailable")
}

// HeartbeatRestart restarts cancels any ongoing heartbeat and restarts it.
// If there is no ongoing heartbeat then this is a no-op.
// Returns true if new heartbeat round was started.
func (g *gateway) HeartbeatRestart() bool {
	heartbeatCancel := g.HearbeatCancelFunc()

	// There is a cancellable heartbeat round ongoing.
	if heartbeatCancel != nil {
		g.heartbeatCancel() // Request ongoing heartbeat round cancel itself.

		// Start a new heartbeat round async that will run as soon as ongoing heartbeat round exits.
		go g.Heartbeat(g.ctx, heartbeat.HeartbeatImmediate)

		return true
	}

	return false
}

// Return information about the cluster members that a currently part of the raft
// cluster, as configured in the raft log. It returns an error if this node is
// not the leader.
func (g *gateway) CurrentRaftNodes(ctx context.Context) ([]db.RaftNode, error) {
	g.lock.RLock()
	defer g.lock.RUnlock()

	if g.info == nil || g.info.Role != db.RaftVoter {
		return nil, membership.ErrNotLeader
	}

	isLeader, err := g.IsLeader(ctx)
	if err != nil {
		return nil, err
	}

	if !isLeader {
		return nil, membership.ErrNotLeader
	}

	cowsqlClient, err := g.RaftClient(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client", "err", err)
		}
	}()

	servers, err := cowsqlClient.Cluster(context.Background())
	if err != nil {
		return nil, err
	}

	raftNodes := make([]db.RaftNode, 0, len(servers))
	for i, server := range servers {
		address, err := g.nodeAddress(context.TODO(), server.Address)
		if err != nil {
			return nil, fmt.Errorf("Failed to fetch raft server address: %w", err)
		}

		servers[i].Address = address

		raftNode := db.RaftNode{NodeInfo: servers[i]}
		raftNodes = append(raftNodes, raftNode)
	}

	// Get the names of the raft nodes from the global database.
	if g.cluster != nil {
		err = transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
			members, err := g.cluster.GetNodes(ctx)
			if err != nil {
				return fmt.Errorf("Failed getting cluster members: %w", err)
			}

			membersByAddress := make(map[string]db.NodeInfo, len(members))
			for _, member := range members {
				membersByAddress[member.Address] = member
			}

			for i, server := range servers {
				member, found := membersByAddress[server.Address]
				if !found {
					slog.Warn("Cluster member info not found", "address", server.Address)
				}

				raftNodes[i].Name = member.Name
			}

			return nil
		})
		if err != nil {
			slog.Warn("Failed getting raft nodes", "err", err)
		}
	}

	return raftNodes, nil
}

// HeartbeatInterval returns heartbeat interval to use.
func (g *gateway) HeartbeatInterval() time.Duration {
	threshold := g.heartbeatOfflineThreshold
	if threshold <= 0 {
		threshold = g.UserConfig().DefaultOfflineThreshold()
	}

	return threshold / 2
}

func (g *gateway) Heartbeat(ctx context.Context, mode heartbeat.Mode) {
	if g.cluster == nil || g.server == nil || g.memoryDial != nil {
		// We're not a raft node or we're not clustered
		return
	}

	// Avoid concurrent heartbeat loops.
	// This is possible when both the regular task and the out of band heartbeat round from a cowsql
	// connection or notification restart both kick in at the same time.
	g.heartbeatLock.Lock()
	defer g.heartbeatLock.Unlock()

	// Acquire the cancellation lock and populate it so that this heartbeat round can be cancelled if a
	// notification cancellation request arrives during the round. Also setup a defer so that the cancellation
	// function is set to nil when this function ends to indicate there is no ongoing heartbeat round.
	g.heartbeatCancelLock.Lock()
	ctx, g.heartbeatCancel = context.WithCancel(ctx)
	g.heartbeatCancelLock.Unlock()

	defer func() {
		heartbeatCancel := g.HearbeatCancelFunc()
		if heartbeatCancel != nil {
			g.heartbeatCancel()
			g.heartbeatCancel = nil
		}
	}()

	raftNodes, err := g.CurrentRaftNodes(context.TODO())
	if err != nil {
		if errors.Is(err, membership.ErrNotLeader) {
			return
		}

		slog.Error("Failed to get current raft members", "err", err)
		return
	}

	// Address of this node.
	localClusterAddress := g.state.ClusterAddress()

	var members []db.NodeInfo

	err = transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
		tx := g.Cluster()
		var err error
		members, err = tx.GetNodes(ctx)

		return err
	})
	if err != nil {
		slog.Warn("Failed to get current cluster members", "err", err)
		return
	}

	modeStr := mode.Name()

	if mode != heartbeat.HeartbeatNormal {
		// Log unscheduled heartbeats with a higher level than normal heartbeats.
		slog.Info("Starting instant heartbeat round", "mode", modeStr)
	} else {
		// Don't spam the normal log with regular heartbeat messages.
		slog.Debug("Starting heartbeat round", "mode", modeStr)
	}

	// Replace the local raft_nodes table immediately because it
	// might miss a row containing ourselves, since we might have
	// been elected leader before the former leader had chance to
	// send us a fresh update through the heartbeat pool.
	slog.Debug("Heartbeat updating local raft members", "members", raftNodes)

	err = transaction.Do(context.TODO(), g.Node(), func(ctx context.Context) error {
		tx := g.Node()
		return tx.ReplaceRaftNodes(ctx, raftNodes)
	})
	if err != nil {
		slog.Warn("Failed to replace local raft members", "err", err, "mode", modeStr)
		return
	}

	if localClusterAddress == "" {
		slog.Error("No local address set, aborting heartbeat round", "mode", modeStr)
		return
	}

	startTime := time.Now()

	heartbeatInterval := g.HeartbeatInterval()

	// Cumulative set of node states (will be written back to database once done).
	hbState := heartbeat.NewAPIHearbeat(g.cluster)

	// If we are doing a normal heartbeat round then spread the requests over the heartbeatInterval in order
	// to reduce load on the cluster.
	spreadDuration := time.Duration(0)
	if mode == heartbeat.HeartbeatNormal {
		spreadDuration = heartbeatInterval
	}

	serverCert := g.serverCert()

	// If this leader node hasn't sent a heartbeat recently, then its node state records
	// are likely out of date, this can happen when a node becomes a leader.
	// Send stale set to all nodes in database to get a fresh set of active nodes.
	if mode == heartbeat.HeartbeatInitial {
		hbState.Update(false, raftNodes, members, g.heartbeatOfflineThreshold)
		hbState.Send(ctx, g.UserConfig().DatabaseEndpoint(), g.networkCert, serverCert, localClusterAddress, members, spreadDuration)

		// We have the latest set of node states now, lets send that state set to all nodes.
		hbState.FullStateList = true
		hbState.Send(ctx, g.UserConfig().DatabaseEndpoint(), g.networkCert, serverCert, localClusterAddress, members, spreadDuration)
	} else {
		hbState.Update(true, raftNodes, members, g.heartbeatOfflineThreshold)
		hbState.Send(ctx, g.UserConfig().DatabaseEndpoint(), g.networkCert, serverCert, localClusterAddress, members, spreadDuration)
	}

	// Check if context has been cancelled.
	ctxErr := ctx.Err()

	// Look for any new node which appeared since sending last heartbeat.
	if ctxErr == nil {
		var currentMembers []db.NodeInfo
		err = transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
			tx := g.Cluster()
			var err error
			currentMembers, err = tx.GetNodes(ctx)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			slog.Warn("Failed to get current cluster members", "err", err, "mode", modeStr)
			return
		}

		newMembers := []db.NodeInfo{}
		for _, currentMember := range currentMembers {
			existing := false
			for _, member := range members {
				if member.Address == currentMember.Address && member.ID == currentMember.ID {
					existing = true
					break
				}
			}

			if !existing {
				// We found a new node
				members = append(members, currentMember)
				newMembers = append(newMembers, currentMember)
			}
		}

		// If any new nodes found, send heartbeat to just them (with full node state).
		if len(newMembers) > 0 {
			hbState.Update(true, raftNodes, members, g.heartbeatOfflineThreshold)
			hbState.Send(ctx, g.UserConfig().DatabaseEndpoint(), g.networkCert, serverCert, localClusterAddress, newMembers, 0)
		}
	}

	// Initialize slice to indicate to HeartbeatNodeHook that its being called from leader.
	unavailableMembers := make([]string, 0)

	err = transaction.Retry(ctx, g.UserConfig().MaxDBRetries(), func(ctx context.Context) error {
		// Durating cluster member fluctuations/upgrades the cluster can become unavailable so check here.
		if g.Cluster() == nil {
			return errors.New("Cluster unavailable")
		}

		return transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
			tx := g.Cluster()
			existingNodes, err := tx.GetNodes(ctx)
			if err != nil {
				return err
			}

			nodesByAddress := make(map[string]struct{}, len(existingNodes))
			for _, n := range existingNodes {
				nodesByAddress[n.Address] = struct{}{}
			}

			for _, node := range hbState.Members {
				if !node.Updated {
					// If member has not been updated during this heartbeat round it means
					// they are currently unreachable or rejecting heartbeats due to being
					// in the process of shutting down. Either way we do not want to use this
					// member as a candidate for role promotion.
					unavailableMembers = append(unavailableMembers, node.Address)
					continue
				}

				_, ok := nodesByAddress[node.Address]
				if ok {
					err := tx.SetNodeHeartbeat(ctx, node.Address, node.LastHeartbeat)
					if err != nil {
						return fmt.Errorf("Failed updating heartbeat time for member %q: %w", node.Address, err)
					}
				}
			}

			return nil
		})
	})
	if err != nil {
		slog.Error("Failed updating cluster heartbeats", "err", err)
		return
	}

	// If the context has been cancelled, return prematurely after saving the members we did manage to ping.
	if ctxErr != nil {
		slog.Warn("Aborting heartbeat round", "err", ctxErr, "mode", modeStr)
		return
	}

	// If full node state was sent and node refresh task is specified.
	if g.heartbeatNodeHook != nil {
		g.heartbeatNodeHook(hbState, true, unavailableMembers)
	}

	duration := time.Since(startTime)
	if duration > heartbeatInterval {
		slog.Warn("Cluster heartbeat took too long", "duration", duration, "interval", heartbeatInterval)
	}

	if mode != heartbeat.HeartbeatNormal {
		// Log unscheduled heartbeats with a higher level than normal heartbeats.
		slog.Info("Completed instant heartbeat round", "duration", duration)
	} else {
		// Don't spam the normal log with regular heartbeat messages.
		slog.Debug("Completed heartbeat round", "duration", duration)
	}
}

func (g *gateway) IsLeader(ctx context.Context) (bool, error) {
	if g.server == nil || g.info.Role != db.RaftVoter {
		return false, nil
	}

	cowsqlClient, err := g.RaftClient(ctx)
	if err != nil {
		return false, fmt.Errorf("Failed to get cowsql client: %w", err)
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client", "err", err)
		}
	}()

	ctx, cancel := context.WithTimeout(g.ctx, 3*time.Second)
	defer cancel()
	leader, err := cowsqlClient.Leader(ctx)
	if err != nil {
		return false, fmt.Errorf("Failed to get leader address: %w", err)
	}

	return leader != nil && leader.ID == g.info.ID, nil
}
