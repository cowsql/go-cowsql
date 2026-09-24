//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cowsql/go-cowsql/cluster"
	exampletls "github.com/cowsql/go-cowsql/cluster/example/tls"
	"github.com/cowsql/go-cowsql/cluster/gateway"
	"github.com/cowsql/go-cowsql/cluster/internal/util/revert"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

// Daemon is an example daemon that sets up the cluster listener, databases, and API.
type Daemon struct {
	cancel         context.CancelFunc
	dir            string
	serverName     string
	clusterAddress string

	// serverListener  *tls.FancyTLSListener
	listener *exampletls.FancyTLSListener
	server   *http.Server
	gateway  cluster.Gateway

	trustedCertsMu sync.Mutex
	trustedCerts   map[string]x509.Certificate

	hbMu sync.Mutex
}

// NewDaemon creates a new uninitialized Daemon with the given fields.
func NewDaemon(clusterAddress string, serverName string, dir string) *Daemon {
	return &Daemon{
		serverName:     serverName,
		clusterAddress: clusterAddress,
		dir:            dir,
		trustedCerts:   map[string]x509.Certificate{},
	}
}

// Start sets up the listeners, API, and database of the daemon. Blocks until the context is canceled.
func (d *Daemon) Start(ctx context.Context) error {
	if d.dir == "" {
		return errors.New("Directory cannot be empty")
	}

	if d.clusterAddress == "" {
		return errors.New("Cluster address cannot be empty")
	}

	if d.serverName == "" {
		return errors.New("Server name cannot be empty")
	}

	ctx, cancel := context.WithCancel(ctx)
	d.cancel = cancel

	reverter := revert.New()
	defer reverter.Fail()

	reverter.Add(func() { _ = d.stop() })

	_, err := os.Stat(d.dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if err != nil {
		err = os.MkdirAll(d.dir, 0o755)
		if err != nil {
			return err
		}
	}

	serverCert, err := exampletls.KeyPairAndCA(d.dir, "server")
	if err != nil {
		return err
	}

	databaseDir := filepath.Join(d.dir, "database")

	err = os.MkdirAll(databaseDir, 0o755)
	if err != nil {
		return err
	}

	localDB, err := OpenLocalDB(databaseDir)
	if err != nil {
		return err
	}

	serverCertFunc := func() tls.CertInfo { return serverCert }

	nodeDB, err := NewNode(ctx, localDB, d.clusterAddress, databaseDir)
	if err != nil {
		return err
	}

	var lc net.ListenConfig

	tcpListener, err := lc.Listen(ctx, "tcp", d.clusterAddress)
	if err != nil {
		return err
	}

	_, err = os.Stat(filepath.Join(d.dir, "cluster.crt"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	var clusterCert tls.CertInfo
	if err != nil {
		clusterCert = serverCert
	} else {
		clusterCert, err = exampletls.KeyPairAndCA(d.dir, "cluster")
		if err != nil {
			return err
		}
	}

	d.listener = exampletls.NewFancyTLSListener(tcpListener, clusterCert)
	daemonState := NewState(nodeDB, d.listener, d.refreshCertificateCache)

	d.gateway, err = gateway.NewGateway(ctx, nodeDB, clusterCert, serverCertFunc, daemonState)
	if err != nil {
		return err
	}

	d.server = &http.Server{ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, Handler: d.apiHandlers()}

	d.refreshCertificateCacheFromLocal(ctx)

	go func() { _ = d.server.Serve(d.listener) }()

	err = d.openCluster(ctx, databaseDir)
	if err != nil {
		return err
	}

	reverter.Success()

	d.gateway.SetHeartbeatNodeHook(d.heartbeatHook)

	go ClusterHeartbeatTask(ctx, d.gateway)

	<-ctx.Done()

	return d.stop()
}

func (d *Daemon) stop() error {
	d.cancel()

	var errs []error

	if d.gateway != nil {
		err := d.gateway.Stop(time.Second * 10)
		if err != nil {
			slog.Error("Failed to stop gateway", slog.Any("error", err))
			errs = append(errs, err)
		}
	}

	if d.server != nil {
		err := d.server.Shutdown(context.Background())
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			slog.Error("Failed to stop server", slog.Any("error", err))
			errs = append(errs, err)
		}
	}

	if d.listener != nil {
		err := d.listener.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("Failed to stop cluster listener", slog.Any("error", err))
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (d *Daemon) openCluster(ctx context.Context, databaseDir string) error {
	drv, err := d.gateway.Driver()
	if err != nil {
		return fmt.Errorf("Failed to create cowsql driver: %w", err)
	}

	db, err := OpenClusterDB(ctx, drv, databaseDir, 36*time.Hour)
	if err != nil {
		return err
	}

	cluster, err := NewCluster(ctx, db)
	if err != nil {
		return err
	}

	d.gateway.SetClusterDB(cluster)

	return nil
}
