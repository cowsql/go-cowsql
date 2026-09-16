package tls

import (
	"bytes"
	"crypto/x509"
	"log/slog"
	"net/http"
	"time"

	"github.com/cowsql/go-cowsql/cluster/tls"
)

// CheckCert checks certificate access, returns true if certificate is trusted.
func CheckCert(r *http.Request, networkCert tls.CertInfo, serverCert tls.CertInfo, trustedCerts map[string]x509.Certificate) bool {
	_, err := x509.ParseCertificate(networkCert.KeyPair().Certificate[0])
	if err != nil {
		// Since we have already loaded this certificate, typically
		// using LoadX509KeyPair, an error should never happen, but
		// check for good measure.
		slog.Error("Invalid network certificate format", "err", err)

		return false
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

		slog.Error("Invalid client certificate", "subject", peerCert.Subject, "fingerprint", tls.CertFingerprint(peerCert), "remote", r.RemoteAddr, "path", r.URL.Path)
	}

	return false
}

// CheckTrustState checks whether the given client certificate is trusted
// (i.e. it has a valid time span and it belongs to the given list of trusted
// certificates).
// Returns whether or not the certificate is trusted, and the fingerprint of the certificate.
func CheckTrustState(cert x509.Certificate, trustedCerts map[string]x509.Certificate, networkCert tls.CertInfo, trustCACertificates bool) (bool, string) {
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
			return true, tls.CertFingerprint(&cert)
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
