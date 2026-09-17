//go:build !nosqlite3 && !darwin

package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

// NewNotifier builds a Notifier that can be used to notify other peers using
// the given policy.
func (g *gateway) NewNotifier(ctx context.Context, networkCert tls.CertInfo, serverCert tls.CertInfo, policy cluster.NotifierPolicy) (cluster.Notifier, error) {
	localClusterAddress, err := g.Node().GetClusterAddress(ctx)
	if err != nil {
		return nil, err
	}

	// Fast-track the case where we're not clustered at all.
	if localClusterAddress == "" || g.Cluster() == nil {
		return func(hook func(ctx context.Context, address string, networkCert tls.CertInfo, serverCert tls.CertInfo) error) []error {
			return nil
		}, nil
	}

	var (
		members          []db.NodeInfo
		offlineThreshold time.Duration
	)

	err = transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
		tx := g.Cluster()

		offlineThreshold, err = tx.GetNodeOfflineThreshold(ctx)
		if err != nil {
			return fmt.Errorf("Failed getting cluster member offline threshold: %w", err)
		}

		members, err = tx.GetNodes(ctx)
		if err != nil {
			return fmt.Errorf("Failed getting cluster members: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	peers := []string{}

	for _, member := range members {
		if member.Address == localClusterAddress || member.Address == "0.0.0.0" {
			continue // Exclude ourselves
		}

		if member.IsOffline(offlineThreshold) {
			// Even if the heartbeat timestamp is not recent
			// enough, let's try to connect to the node, just in
			// case the heartbeat is lagging behind for some reason
			// and the node is actually up.
			switch policy {
			case cluster.NotifyAll:
				if !cluster.HasConnectivity(networkCert, serverCert, member.Address, g.Options().RestrictTLS()) {
					return nil, fmt.Errorf("peer node %s is down", member.Address)
				}
			case cluster.NotifyAlive:
				continue // Just skip this node
			case cluster.NotifyTryAll:
			}
		}

		peers = append(peers, member.Address)
	}

	notifier := func(hook func(ctx context.Context, address string, networkCert, serverCert tls.CertInfo) error) []error {
		errs := make([]error, 0, len(peers))
		wg := sync.WaitGroup{}
		wg.Add(len(peers))

		var mu sync.Mutex

		for _, address := range peers {
			slog.Debug("Notify node of state changes", "address", address)
			go func(address string) {
				defer wg.Done()

				err := hook(ctx, address, networkCert, serverCert)
				if err != nil {
					mu.Lock()

					errs = append(errs, fmt.Errorf("failed to notify peer %s: %w", address, err))
					mu.Unlock()
				}
			}(address)
		}

		wg.Wait()

		return errs
	}

	return notifier, nil
}
