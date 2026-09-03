package cluster

import (
	"context"
	"net/http"
	"time"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/options"
	"github.com/cowsql/go-cowsql/cluster/state"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

// Gateway represents the cluster gateway implementation.
type Gateway interface {
	HandlerFuncs(auth func(w http.ResponseWriter, r *http.Request) bool) map[string]http.HandlerFunc
	WaitUpgradeNotification()
	Initialized() bool
	DialFunc() client.DialFunc
	Context() context.Context
	NodeStore() client.NodeStore
	TransferLeadership(ctx context.Context) error
	DemoteOfflineNode(raftID uint64) error
	Cancel()
	ShutdownServer() error
	Stop(connTimeout time.Duration) error
	Sync()
	Reset(networkCert tls.CertInfo) error
	HearbeatCancelFunc() func()
	NetworkUpdateCert(cert tls.CertInfo)
	WaitLeadership() error
	LeaderAddress() (string, error)
	HeartbeatRestart() bool
	Node() db.Node
	Cluster() db.Cluster
	Initialize(bootstrap bool) error
	CurrentRaftNodes(ctx context.Context) ([]db.RaftNode, error)
	UserConfig() *options.Options
	HeartbeatOfflineThreshold() time.Duration
	RaftDial() client.DialFunc
	Standalone() bool
	NetworkCert() tls.CertInfo
	RaftNode() *db.RaftNode
	SetRaftNode(n *db.RaftNode)
	RaftClient(ctx context.Context) (*client.Client, error)
	AwaitHeartbeat()
	SetClusterDB(db.Cluster)
	HeartbeatInterval() time.Duration
	Heartbeat(ctx context.Context, m heartbeat.Mode)
	SetHeartbeatOfflineThreshold(time.Duration)
	SetHeartbeatNodeHook(f heartbeat.Hook)
	State() state.State
	ServerCert() tls.CertInfo
	NewNotifier(ctx context.Context, networkCert tls.CertInfo, serverCert tls.CertInfo, policy NotifierPolicy) (Notifier, error)
	IsLeader(ctx context.Context) (bool, error)
}
