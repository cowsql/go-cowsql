package cluster

import (
	"context"
	"net"
	"time"

	"github.com/cowsql/go-cowsql/cluster/tls"
)

// HasConnectivity probes the member with the given address for connectivity.
func HasConnectivity(networkCert tls.CertInfo, serverCert tls.CertInfo, address string, useTLS12 bool) bool {
	// Get the transport.
	transport, cleanup, err := tls.Transport(networkCert, serverCert, useTLS12)
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
