package tls

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	incustls "github.com/lxc/incus/v7/shared/tls"
)

// Return a TLS configuration suitable for establishing intra-member network connections using the server cert.
func ClientConfig(networkCert *incustls.CertInfo, serverCert *incustls.CertInfo) (*tls.Config, error) {
	if networkCert == nil {
		return nil, errors.New("Invalid networkCert")
	}

	if serverCert == nil {
		return nil, errors.New("Invalid serverCert")
	}

	keypair := serverCert.KeyPair()
	config := incustls.InitTLSConfig()
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

// CheckCert checks certificate access, returns true if certificate is trusted.
func CheckCert(r *http.Request, networkCert *incustls.CertInfo, serverCert *incustls.CertInfo, trustedCerts map[string]x509.Certificate) bool {
	_, err := x509.ParseCertificate(networkCert.KeyPair().Certificate[0])
	if err != nil {
		// Since we have already loaded this certificate, typically
		// using LoadX509KeyPair, an error should never happen, but
		// check for good measure.
		panic(fmt.Sprintf("Invalid keypair material: %v", err))
	}

	if r.TLS == nil {
		return false
	}

	for _, peerCert := range r.TLS.PeerCertificates {
		// Trust our own server certificate. This allows Cowsql to start with a connection back to this
		// member before the database is available. It also allows us to switch the server certificate to
		// the network certificate during cluster upgrade to per-server certificates, and it be trusted.
		trustedServerCert, _ := x509.ParseCertificate(serverCert.KeyPair().Certificate[0])
		trusted, _ := CheckTrustState(*peerCert, map[string]x509.Certificate{serverCert.Fingerprint(): *trustedServerCert}, networkCert, false)
		if trusted {
			return true
		}

		// Check the trusted server certificates list provided.
		trusted, _ = CheckTrustState(*peerCert, trustedCerts, networkCert, false)
		if trusted {
			return true
		}

		slog.Error("Invalid client certificate", "subject", peerCert.Subject, "fingerprint", incustls.CertFingerprint(peerCert), "remote", r.RemoteAddr, "path", r.URL.Path)
	}

	return false
}

// Return an http.Transport configured using the given configuration and a
// cleanup function to use to close all connections the transport has been
// used.
func Transport(networkCert *incustls.CertInfo, serverCert *incustls.CertInfo) (*http.Transport, func(), error) {
	config, err := ClientConfig(networkCert, serverCert)
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

	transport.DialTLSContext = func(ctx context.Context, network string, addr string) (net.Conn, error) {
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

		certBlock, _ := pem.Decode([]byte(networkCert.PublicKey()))
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

// CheckTrustState checks whether the given client certificate is trusted
// (i.e. it has a valid time span and it belongs to the given list of trusted
// certificates).
// Returns whether or not the certificate is trusted, and the fingerprint of the certificate.
func CheckTrustState(cert x509.Certificate, trustedCerts map[string]x509.Certificate, networkCert *incustls.CertInfo, trustCACertificates bool) (bool, string) {
	// Extra validity check (should have been caught by TLS stack)
	if time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
		return false, ""
	}

	if networkCert != nil && trustCACertificates {
		ca := networkCert.CA()

		if ca != nil && cert.CheckSignatureFrom(ca) == nil {
			// Check whether the certificate has been revoked.
			crl := networkCert.CRL()

			if crl != nil {
				if crl.CheckSignatureFrom(ca) != nil {
					return false, "" // CRL not signed by CA
				}

				for _, revoked := range crl.RevokedCertificateEntries {
					if cert.SerialNumber.Cmp(revoked.SerialNumber) == 0 {
						return false, "" // Certificate is revoked, so not trusted anymore.
					}
				}
			}

			// Certificate not revoked, so trust it as is signed by CA cert.
			return true, incustls.CertFingerprint(&cert)
		}
	}

	// Check whether client certificate is in trust store.
	for fingerprint, v := range trustedCerts {
		if bytes.Equal(cert.Raw, v.Raw) {
			slog.Debug("Matched trusted cert", "fingerprint", fingerprint, "subject", v.Subject)
			return true, fingerprint
		}
	}

	return false, ""
}
