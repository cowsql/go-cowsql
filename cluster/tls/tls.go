package tls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// InitTLSConfig returns a tls.Config populated with default encryption
// parameters. This is used as baseline config for both client and server
// certificates.
func InitTLSConfig(useTLS12 bool) *tls.Config {
	config := &tls.Config{}

	// Restrict to TLS 1.3 unless INCUS_INSECURE_TLS is set.
	if !useTLS12 {
		config.MinVersion = tls.VersionTLS13
	} else {
		config.MinVersion = tls.VersionTLS12
	}

	return config
}

// ClientConfig returns a TLS configuration suitable for establishing intra-member network connections using the server cert.
func ClientConfig(networkCert CertInfo, serverCert CertInfo, useTLS12 bool) (*tls.Config, error) {
	if networkCert == nil {
		return nil, errors.New("Invalid networkCert")
	}

	if serverCert == nil {
		return nil, errors.New("Invalid serverCert")
	}

	keypair := serverCert.KeyPair()
	config := InitTLSConfig(useTLS12)
	config.Certificates = []tls.Certificate{keypair}
	config.RootCAs = x509.NewCertPool()

	ca := serverCert.CA()
	if ca != nil {
		config.RootCAs.AddCert(ca)
	}

	// Since the same cluster keypair is used both as server and as client
	// cert, let's add it to the CA pool to make it trusted.
	networkKeypair := networkCert.KeyPair()

	netCert, err := x509.ParseCertificate(networkKeypair.Certificate[0])
	if err != nil {
		return nil, err
	}

	netCert.IsCA = true
	netCert.KeyUsage = x509.KeyUsageCertSign
	config.RootCAs.AddCert(netCert)

	// Always use network certificate's DNS name rather than server cert, so that it matches.
	if len(netCert.DNSNames) > 0 {
		config.ServerName = netCert.DNSNames[0]
	}

	return config, nil
}

// Transport returns an http.Transport configured using the given configuration and a
// cleanup function to use to close all connections the transport has been
// used.
func Transport(networkCert CertInfo, serverCert CertInfo, useTLS12 bool) (*http.Transport, func(), error) {
	config, err := ClientConfig(networkCert, serverCert, useTLS12)
	if err != nil {
		return nil, nil, err
	}

	// Set InsecureSkipVerify as we're doing our own certificate validation below.
	config.InsecureSkipVerify = true

	transport := &http.Transport{
		TLSClientConfig:       config,
		DisableKeepAlives:     true,
		MaxIdleConns:          0,
		ExpectContinueTimeout: time.Second * 30,
		ResponseHeaderTimeout: time.Second * 3600,
		TLSHandshakeTimeout:   time.Second * 5,
	}

	transport.DialTLSContext = func(ctx context.Context, _ string, addr string) (net.Conn, error) {
		// Establish the TCP connection.
		dialer := net.Dialer{Timeout: 10 * time.Second}

		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("Failed connecting to HTTPS endpoint [%q]: %w", addr, err)
		}

		// Get TLS connection.
		tlsConn := tls.Client(conn, config)

		// Validate the connection (TLSHandshakeTimeout doesn't apply to DialTLSContext).
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		err = tlsConn.HandshakeContext(ctx)
		if err != nil {
			_ = conn.Close()

			return nil, err
		}

		_ = conn.SetDeadline(time.Time{})

		// Look for an exact match with the certificate provided.
		// But ignore any other issue (validity, scope, ...).
		cs := tlsConn.ConnectionState()

		if len(cs.PeerCertificates) < 1 {
			return nil, errors.New("Couldn't validate peer certificate")
		}

		certBlock, _ := pem.Decode(networkCert.PublicKey())
		if certBlock == nil {
			return nil, errors.New("Invalid remote certificate")
		}

		expectedRemoteCert, err := x509.ParseCertificate(certBlock.Bytes)
		if err != nil {
			return nil, err
		}

		if !cs.PeerCertificates[0].Equal(expectedRemoteCert) {
			return nil, errors.New("Remote certificate differs from expected")
		}

		return tlsConn, nil
	}

	return transport, transport.CloseIdleConnections, nil
}
