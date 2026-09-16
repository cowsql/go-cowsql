//go:build !nosqlite3 && !darwin

package example

import (
	"context"
	"crypto/x509"
	"log/slog"
	"maps"
	"slices"

	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/example/query"
	"github.com/cowsql/go-cowsql/cluster/example/tls"
)

// GetLocalCertificates is a helper to fetch persisted server certificates from the local cache of cluster members.
func GetLocalCertificates(ctx context.Context, n db.Node) (map[string]tls.Certificate, error) {
	certsByAddr := map[string]tls.Certificate{}

	err := transaction.ForceTx(ctx, n, func(ctx context.Context, tx transaction.TX) error {
		var scan query.Dest = func(scan func(dest ...any) error) error {
			var (
				address string
				cert    tls.Certificate
			)

			err := scan(&address, &cert)
			if err != nil {
				return err
			}

			certsByAddr[address] = cert

			return nil
		}

		return query.SelectObjects(ctx, tx, "SELECT address, certificate FROM raft_nodes WHERE address != '1' AND certificate IS NOT NULL", scan)
	})
	if err != nil {
		return nil, err
	}

	return certsByAddr, nil
}

// updateLocalCertificate is a helper to set the server certificate for a cluster member in the local cache.
func (d *Daemon) updateLocalCertificate(ctx context.Context, address string, cert tls.Certificate) error {
	return transaction.ForceTx(ctx, d.gateway.Node(), func(ctx context.Context, tx transaction.TX) error {
		_, err := tx.ExecContext(ctx, "UPDATE raft_nodes SET certificate = ? WHERE address = ?", cert, address)

		return err
	})
}

// getGlobalCertificates fetches a map of server certificates keyed by cluster member address.
func (d *Daemon) getGlobalCertificates(ctx context.Context) (map[string]tls.Certificate, error) {
	certsByAddress := map[string]tls.Certificate{}

	err := transaction.ForceTx(ctx, d.gateway.Cluster(), func(ctx context.Context, tx transaction.TX) error {
		var scan query.Dest = func(scan func(dest ...any) error) error {
			var (
				address string
				cert    tls.Certificate
			)

			err := scan(&address, &cert)
			if err != nil {
				return err
			}

			certsByAddress[address] = cert

			return nil
		}

		return query.SelectObjects(ctx, tx, "SELECT address, certificate FROM nodes WHERE address != '0.0.0.0' AND certificate is not NULL", scan)
	})
	if err != nil {
		return nil, err
	}

	return certsByAddress, nil
}

// setCertificateCache updates the in-memory list of server certificates, used for intra-cluster communication.
func (d *Daemon) setCertificateCache(certs []tls.Certificate) {
	d.trustedCertsMu.Lock()
	defer d.trustedCertsMu.Unlock()

	for _, cert := range certs {
		fp, err := cert.Fingerprint()
		if err != nil {
			slog.Error("Certificate is invalid", slog.Any("error", err))

			continue
		}

		d.trustedCerts[fp] = *cert.Certificate
	}
}

// refreshCertificateCacheFromLocal updates the in-memory cache of server certificates according to the current state of the local database.
// This is useful when starting up the daemon, before the cluster database has started.
func (d *Daemon) refreshCertificateCacheFromLocal(ctx context.Context) {
	var certsByAddress map[string]tls.Certificate

	err := transaction.ForceTx(ctx, d.gateway.Node(), func(ctx context.Context, tx transaction.TX) error {
		var err error

		certsByAddress, err = GetLocalCertificates(ctx, d.gateway.Node())

		return err
	})
	if err != nil {
		slog.Error("Failed to retrieve server certificates from local cluster members", slog.Any("error", err))
	}

	d.setCertificateCache(slices.Collect(maps.Values(certsByAddress)))
}

// refreshCertificateCache is a helper to update the in-memory cache of server certificates according to the current state of cluster members in the global database.
func (d *Daemon) refreshCertificateCache(ctx context.Context) {
	cluster := d.gateway.Cluster()
	if cluster == nil {
		return
	}

	var certsByAddress map[string]tls.Certificate

	err := transaction.ForceTx(ctx, cluster, func(ctx context.Context, tx transaction.TX) error {
		var err error

		certsByAddress, err = d.getGlobalCertificates(ctx)

		return err
	})
	if err != nil {
		slog.Error("Failed to retrieve server certificates from cluster members", slog.Any("error", err))
	}

	d.setCertificateCache(slices.Collect(maps.Values(certsByAddress)))
}

// getTrustedCerts returns the in-memory cache of server certificates.
func (d *Daemon) getTrustedCerts() map[string]x509.Certificate {
	d.trustedCertsMu.Lock()
	defer d.trustedCertsMu.Unlock()

	certsCopy := make(map[string]x509.Certificate, len(d.trustedCerts))
	maps.Copy(certsCopy, d.trustedCerts)

	return certsCopy
}
