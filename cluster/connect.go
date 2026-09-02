package cluster

import (
	"context"
	"net"
	"time"

	"github.com/cowsql/go-cowsql/cluster/tls"
	incustls "github.com/lxc/incus/v7/shared/tls"
)

// HasConnectivity probes the member with the given address for connectivity.
func HasConnectivity(networkCert *incustls.CertInfo, serverCert *incustls.CertInfo, address string) bool {
	// Get the transport.
	transport, cleanup, err := tls.Transport(networkCert, serverCert)
	if err != nil {
		return false
	}

	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var conn net.Conn
	conn, err = transport.DialTLSContext(ctx, "tcp", address)
	if err == nil {
		_ = conn.Close()
		return true
	}

	return false
}
