package cluster

import (
	"context"
	"crypto/x509"
	"net/http"
	"time"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/options"
	"github.com/cowsql/go-cowsql/cluster/state"
	incustls "github.com/lxc/incus/v7/shared/tls"
)

// Gateway represents the cluster gateway implementation.
type Gateway interface {
	HandlerFuncs(trustedCerts func() (map[string]x509.Certificate, error)) map[string]http.HandlerFunc
	WaitUpgradeNotification()
	IsCowsqlNode() bool
	DialFunc() client.DialFunc
	Context() context.Context
	NodeStore() client.NodeStore
	TransferLeadership(ctx context.Context) error
	DemoteOfflineNode(raftID uint64) error
	Kill()
	Shutdown() error
	Sync()
	Reset(networkCert *incustls.CertInfo) error
	HearbeatCancelFunc() func()
	NetworkUpdateCert(cert *incustls.CertInfo)
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
	NetworkCert() *incustls.CertInfo
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
	ServerCert() *incustls.CertInfo
	NewNotifier(ctx context.Context, networkCert *incustls.CertInfo, serverCert *incustls.CertInfo, policy NotifierPolicy) (Notifier, error)
	IsLeader(ctx context.Context) (bool, error)
}
