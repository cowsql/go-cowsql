package tls

import (
	"crypto/tls"
	"net"
	"sync"

	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
)

// FancyTLSListener is a variation of the standard tls.Listener that supports
// atomically swapping the underlying TLS configuration and proxy protocol wrapping.
// Requests served before the swap will continue using the old configuration.
type FancyTLSListener struct {
	net.Listener

	mu     sync.RWMutex
	config *tls.Config
}

// NewFancyTLSListener creates a new FancyTLSListener.
func NewFancyTLSListener(inner net.Listener, cert cowsqltls.CertInfo) *FancyTLSListener {
	listener := &FancyTLSListener{
		Listener: inner,
	}

	listener.Config(cert)

	return listener
}

// Accept waits for and returns the next incoming TLS connection then use the
// current TLS configuration to handle it.
func (l *FancyTLSListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	config := l.config

	return tls.Server(c, config), nil
}

// Config safely swaps the underlying TLS configuration.
func (l *FancyTLSListener) Config(cert cowsqltls.CertInfo) {
	config := ServerTLSConfig(cert)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.config = config
}
