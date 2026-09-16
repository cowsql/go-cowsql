package tls

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// CertInfo captures TLS certificate information about a certain public/private
// keypair and an optional CA certificate and CRL.
//
// Given support for PKI setups, these few bits of information are
// normally used and passed around together, so this structure helps with that
// (see doc/security.md for more details).
type CertInfo interface {
	KeyPair() tls.Certificate
	CA() *x509.Certificate
	CRL() *x509.RevocationList
	PublicKey() []byte
	PublicKeyX509() (*x509.Certificate, error)
	PrivateKey() []byte
	Fingerprint() string
}

type certInfo struct {
	keypair tls.Certificate
	ca      *x509.Certificate
	crl     *x509.RevocationList
}

// NewCertInfo initializes a new CertInfo.
func NewCertInfo(keypair tls.Certificate, ca *x509.Certificate, crl *x509.RevocationList) CertInfo {
	return &certInfo{
		keypair: keypair,
		ca:      ca,
		crl:     crl,
	}
}

// KeyPair returns the public/private key pair.
func (c *certInfo) KeyPair() tls.Certificate {
	return c.keypair
}

// CA returns the CA certificate.
func (c *certInfo) CA() *x509.Certificate {
	return c.ca
}

// PublicKey is a convenience to encode the underlying public key to ASCII.
func (c *certInfo) PublicKey() []byte {
	data := c.KeyPair().Certificate[0]

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: data})
}

// PublicKeyX509 is a convenience to return the underlying public key as an *x509.Certificate.
func (c *certInfo) PublicKeyX509() (*x509.Certificate, error) {
	return x509.ParseCertificate(c.KeyPair().Certificate[0])
}

// PrivateKey is a convenience to encode the underlying private key.
func (c *certInfo) PrivateKey() []byte {
	ecKey, ok := c.KeyPair().PrivateKey.(*ecdsa.PrivateKey)
	if ok {
		data, err := x509.MarshalECPrivateKey(ecKey)
		if err != nil {
			return nil
		}

		return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: data})
	}

	rsaKey, ok := c.KeyPair().PrivateKey.(*rsa.PrivateKey)
	if ok {
		data := x509.MarshalPKCS1PrivateKey(rsaKey)

		return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: data})
	}

	return nil
}

// Fingerprint returns the fingerprint of the public key.
func (c *certInfo) Fingerprint() string {
	fingerprint, err := CertFingerprintStr(string(c.PublicKey()))
	// Parsing should never fail, since we generated the cert ourselves,
	// but let's check the error for good measure.
	if err != nil {
		panic("invalid public key material")
	}

	return fingerprint
}

// CRL returns the certificate revocation list.
func (c *certInfo) CRL() *x509.RevocationList {
	return c.crl
}

// CertFingerprintStr returns the SHA256 fingerprint of a PEM encoded certificate.
func CertFingerprintStr(c string) (string, error) {
	pemCertificate, _ := pem.Decode([]byte(c))
	if pemCertificate == nil {
		return "", errors.New("invalid certificate")
	}

	cert, err := x509.ParseCertificate(pemCertificate.Bytes)
	if err != nil {
		return "", err
	}

	return CertFingerprint(cert), nil
}

// CertFingerprint returns the SHA256 fingerprint string of an x509 certificate.
func CertFingerprint(cert *x509.Certificate) string {
	return fmt.Sprintf("%x", sha256.Sum256(cert.Raw))
}
