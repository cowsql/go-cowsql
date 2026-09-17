//go:build !nosqlite3 && !darwin

package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster/api"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/internal/util/file"
	"github.com/cowsql/go-cowsql/cluster/internal/util/revert"
	"github.com/cowsql/go-cowsql/cluster/internal/util/tcp"
	"github.com/cowsql/go-cowsql/cluster/membership"
	"github.com/cowsql/go-cowsql/cluster/options"
	"github.com/cowsql/go-cowsql/cluster/state"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

type gateway struct {
	options *options.Options

	db          db.Node
	networkCert tls.CertInfo
	serverCert  func() tls.CertInfo

	// Keep track of skews.
	timeSkew bool

	// The raft instance to use for creating the cowsql driver. It's nil if
	// this member is not supposed to be part of the raft cluster.
	info *db.RaftNode

	// The gRPC server exposing the cowsql driver created by this
	// gateway. It's nil if this member is not supposed to be part of the
	// raft cluster.
	server   *cowsql.Node
	acceptCh chan net.Conn
	stopCh   chan struct{}

	// A dialer that will connect to the cowsql server using a loopback
	// net.Conn. It's non-nil when clustering is not enabled on this member
	// and so we don't expose any cowsql or raft network endpoint,
	// but still we want to use cowsql as backend for the "cluster"
	// database, to minimize the difference between code paths in
	// clustering and non-clustering modes.
	memoryDial client.DialFunc

	// Used when shutting down the daemon to cancel any ongoing gRPC
	// dialing attempt.
	shutdownCtx context.Context
	ctx         context.Context
	cancel      context.CancelFunc

	// Used to unblock nodes that are waiting for other nodes to upgrade
	// their version.
	upgradeCh chan struct{}

	// Used to track whether we already triggered an upgrade because we
	// detected a peer with an higher version.
	upgradeTriggered bool

	// Used for the heartbeat handler
	cluster                   db.Cluster
	heartbeatNodeHook         heartbeat.Hook
	heartbeatOfflineThreshold time.Duration
	heartbeatCancel           context.CancelFunc
	heartbeatCancelLock       sync.Mutex
	heartbeatLock             sync.Mutex

	// NodeStore wrapper.
	store *cowsqlNodeStore

	lock sync.RWMutex

	// Abstract unix socket that the local cowsql task is listening to.
	bindAddress string

	// State function.
	state state.State
}

// Initialize the gateway, creating a new raft factory and gRPC server (if this
// node is a database node), and a gRPC dialer.
// @bootstrap should only be true when turning a non-clustered server into
// the first (and leader) member of a new cluster.
func (g *gateway) init(bootstrap bool) error {
	slog.Debug("Initializing database gateway")

	g.stopCh = make(chan struct{})

	info, err := loadInfo(g.db)
	if err != nil {
		return fmt.Errorf("Failed to create raft factory: %w", err)
	}

	if file.PathExists(g.db.LogPath()) {
		return errors.New("Unsupported upgrade path, please reinstall")
	}

	// If the resulting raft instance is not nil, it means that this node
	// should serve as database node, so create a cowsql driver possibly
	// exposing it over the network.
	if info != nil {
		// Use the autobind feature of abstract unix sockets to get a
		// random unused address.
		var lc net.ListenConfig

		listener, err := lc.Listen(g.ctx, "unix", "")
		if err != nil {
			return fmt.Errorf("Failed to autobind unix socket: %w", err)
		}

		g.bindAddress = listener.Addr().String()
		_ = listener.Close()

		options := []cowsql.Option{
			cowsql.WithBindAddress(g.bindAddress),
		}

		if info.Address == "1" {
			if info.ID != 1 {
				return errors.New("Invalid database state, multiple non-initialized member records found")
			}

			g.memoryDial = cowsqlMemoryDial(g.bindAddress)
			g.store.inMemory = client.NewInmemNodeStore()

			err = g.store.Set(context.Background(), []client.NodeInfo{{ID: info.ID, Address: info.Address, Role: info.Role}})
			if err != nil {
				return fmt.Errorf("Failed setting node info in store: %w", err)
			}
		} else {
			go runCowsqlProxy(g.stopCh, g.bindAddress, g.acceptCh)

			g.store.inMemory = nil
			options = append(options, cowsql.WithDialFunc(g.raftDial()))
		}

		server, err := cowsql.New(
			info.ID,
			info.Address,
			g.db.GlobalDatabaseDir(),
			options...,
		)
		if err != nil {
			return fmt.Errorf("Failed to create cowsql server: %w", err)
		}

		// Force the correct configuration into the bootstrap node, this is needed
		// when the raft node already has log entries, in which case a regular
		// bootstrap fails, resulting in the node containing outdated configuration.
		if bootstrap {
			slog.Debug("Bootstrap database gateway", "id", info.ID, "address", info.Address)
			cluster := []cowsql.NodeInfo{
				{ID: info.ID, Address: info.Address},
			}

			err = server.Recover(cluster) //nolint:staticcheck
			if err != nil {
				return fmt.Errorf("Failed to recover database state: %w", err)
			}
		}

		err = server.Start()
		if err != nil {
			return fmt.Errorf("Failed to start cowsql server: %w", err)
		}

		g.lock.Lock()
		g.server = server
		g.info = info
		g.lock.Unlock()
	} else {
		g.lock.Lock()
		g.server = nil
		g.info = nil
		g.store.inMemory = nil
		g.lock.Unlock()
	}

	g.lock.Lock()
	g.store.onDisk = client.NewNodeStore(
		g.db.DB(), "main", "raft_nodes", "address",
	)
	g.lock.Unlock()

	return nil
}

// Create a dial function that connects to the local cowsql.
func cowsqlMemoryDial(bindAddress string) client.DialFunc {
	return func(ctx context.Context, address string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", bindAddress)
	}
}

// Translate a raft address to a node address. They are always the same except
// for the bootstrap node, which has address "1".
func (g *gateway) nodeAddress(ctx context.Context, raftAddress string) (string, error) {
	if raftAddress != "1" && raftAddress != "0" {
		return raftAddress, nil
	}

	var address string

	err := transaction.Do(ctx, g.Node(), func(ctx context.Context) error {
		var err error

		raftNode, found, err := g.db.GetRaftNode(ctx, 1)
		if err != nil {
			return fmt.Errorf("Failed to fetch raft server address: %w", err)
		}

		if !found {
			// Use the initial address as fallback. This is an edge
			// case that happens when listing members on a
			// non-clustered node.
			address = raftAddress
		} else {
			address = raftNode.Address
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	return address, nil
}

// Dial function for establishing raft connections.
func (g *gateway) raftDial() client.DialFunc {
	return func(ctx context.Context, address string) (net.Conn, error) {
		nodeAddress, err := g.nodeAddress(context.TODO(), address)
		if err != nil {
			return nil, err
		}

		conn, err := cowsqlNetworkDial(ctx, "raft", nodeAddress, g)
		if err != nil {
			return nil, err
		}

		var lc net.ListenConfig

		listener, err := lc.Listen(ctx, "unix", "")
		if err != nil {
			return nil, fmt.Errorf("Failed to create unix listener: %w", err)
		}

		var d net.Dialer

		goUnix, err := d.DialContext(ctx, "unix", listener.Addr().String())
		if err != nil {
			return nil, fmt.Errorf("Failed to connect to unix listener: %w", err)
		}

		cUnix, err := listener.Accept()
		if err != nil {
			return nil, fmt.Errorf("Failed to connect to unix listener: %w", err)
		}

		_ = listener.Close()

		go cowsqlProxy("raft", g.stopCh, conn, goUnix)

		return cUnix, nil
	}
}

func cowsqlNetworkDial(ctx context.Context, name string, addr string, g *gateway) (net.Conn, error) {
	transport, cleanup, err := tls.Transport(g.networkCert, g.serverCert(), g.Options().RestrictTLS())
	if err != nil {
		return nil, err
	}

	defer cleanup()

	path := fmt.Sprintf("https://%s%s", addr, g.Options().DatabaseEndpoint())

	// Establish the connection
	req := &http.Request{
		Method:     http.MethodPost,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       addr,
	}

	req.URL, err = url.Parse(path)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Upgrade", "dqlite")
	api.SetCOWSQLVersionHeader(req)
	req = req.WithContext(ctx)

	reverter := revert.New()
	defer reverter.Fail()

	conn, err := transport.DialTLSContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	reverter.Add(func() { _ = conn.Close() })

	l := slog.With("name", name, "local", conn.LocalAddr(), "remote", conn.RemoteAddr())
	l.Debug("Cowsql connected outbound")

	remoteTCP, err := tcp.ExtractConn(conn)
	if err != nil {
		l.Warn("Failed extracting TCP connection from remote connection", "err", err)
	} else {
		err := tcp.SetTimeouts(remoteTCP, time.Second*30)
		if err != nil {
			l.Warn("Failed setting TCP timeouts on remote connection", "err", err)
		}
	}

	err = req.Write(conn)
	if err != nil {
		return nil, fmt.Errorf("Failed sending HTTP request to %q: %w", req.URL, err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response: %w", err)
	}

	defer resp.Body.Close()

	// If the remote server has detected that we are out of date, let's
	// trigger an upgrade.
	if resp.StatusCode == http.StatusUpgradeRequired {
		g.lock.Lock()
		defer g.lock.Unlock()

		if !g.upgradeTriggered {
			err = membership.TriggerUpdate(g)
			if err == nil {
				g.upgradeTriggered = true
			}
		}

		return nil, errors.New("Upgrade needed")
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("Dialing failed: expected status code 101 got %d", resp.StatusCode)
	}

	if resp.Header.Get("Upgrade") != "dqlite" {
		return nil, errors.New("Missing or unexpected Upgrade header in response")
	}

	reverter.Success()

	return conn, nil
}

// heartbeatHandler handles heartbeat requests from other cluster members.
func (g *gateway) heartbeatHandler(w http.ResponseWriter, _ *http.Request, isLeader bool, hbData *heartbeat.APIHeartbeat) {
	var err error

	// Look for time skews.
	now := time.Now().UTC()

	if hbData.Time.Add(5*time.Second).Before(now) || hbData.Time.Add(-5*time.Second).After(now) {
		if !g.timeSkew {
			slog.Warn("Time skew detected between leader and local", "leaderTime", hbData.Time, "localTime", now)

			if g.Cluster() != nil {
				warnings, ok := g.Cluster().(db.ClusterWarningHandler)
				if ok {
					err := transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
						return warnings.EmitTimeSkewWarning(ctx, g.info.Name, fmt.Sprintf("leaderTime: %s, localTime: %s", hbData.Time, now))
					})
					if err != nil {
						slog.Warn("Failed to create cluster time skew warning", "err", err)
					}
				}
			}
		}

		g.timeSkew = true
	} else if g.timeSkew {
		slog.Warn("Time skew resolved")

		if g.Cluster() != nil {
			warnings, ok := g.Cluster().(db.ClusterWarningHandler)
			if ok {
				err := transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
					return warnings.ResolveTimeSkewWarning(ctx, g.info.Name)
				})
				if err != nil {
					slog.Warn("Failed to resolve cluster time skew warning", "err", err)
				}
			}
		}

		g.timeSkew = false
	}

	// Extract the raft nodes from the heartbeat info.
	raftNodes := make([]db.RaftNode, 0)

	for _, member := range hbData.Members {
		if member.RaftID > 0 {
			raftNodes = append(raftNodes, db.RaftNode{
				ID:      member.RaftID,
				Address: member.Address,
				Role:    db.RaftRole(member.RaftRole),
				Name:    member.Name,
			})
		}
	}

	// Check we have been sent at least 1 raft node before wiping our set.
	if len(raftNodes) == 0 {
		slog.Error("Empty raft member set received")
		http.Error(w, "400 Empty raft member set received", http.StatusBadRequest)

		return
	}

	// Accept raft node list from any heartbeat type so that we get freshest data quickly.
	slog.Debug("Replace current raft nodes", "raft_members", raftNodes)

	err = transaction.Do(context.TODO(), g.Node(), func(ctx context.Context) error {
		return g.Node().ReplaceRaftNodes(ctx, raftNodes)
	})
	if err != nil {
		slog.Error("Error updating raft members", "err", err)
		http.Error(w, "500 failed to update raft nodes", http.StatusInternalServerError)

		return
	}

	if hbData.FullStateList {
		// If there is an ongoing heartbeat round (and by implication this is the leader), then this could
		// be a problem because it could be broadcasting the stale member state information which in turn
		// could lead to incorrect decisions being made. So calling heartbeatRestart will request any
		// ongoing heartbeat round to cancel itself prematurely and restart another one. If there is no
		// ongoing heartbeat round or this member isn't the leader then this function call is a no-op and
		// will return false. If the heartbeat is restarted, then the heartbeat refresh task will be called
		// at the end of the heartbeat so no need to do it here.
		if (!isLeader || !g.HeartbeatRestart()) && g.heartbeatNodeHook != nil {
			// Run heartbeat refresh task async so heartbeat response is sent to leader straight away.
			go g.heartbeatNodeHook(hbData, isLeader, nil)
		}
	} else {
		if isLeader {
			slog.Error("Partial heartbeat should not be sent to leader")
			http.Error(w, "400 Partial heartbeat should not be sent to leader", http.StatusBadRequest)

			return
		}

		slog.Debug("Partial heartbeat received")
	}
}
