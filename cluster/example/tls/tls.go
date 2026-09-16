package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/cowsql/go-cowsql/cluster/internal/util/file"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
)

// ServerTLSConfig returns a new server-side tls.Config generated from the give
// certificate info.
func ServerTLSConfig(cert cowsqltls.CertInfo) *tls.Config {
	config := &tls.Config{MinVersion: tls.VersionTLS13}
	config.ClientAuth = tls.RequestClientCert
	config.Certificates = []tls.Certificate{cert.KeyPair()}
	config.NextProtos = []string{"h2"} // Required by gRPC

	return config
}

// KeyPairAndCA returns a CertInfo object with a reference to the key pair and
// (optionally) CA certificate located in the given directory and having the
// given name prefix
//
// The naming conversion for the various PEM encoded files is:
//
// <prefix>.crt -> public key
// <prefix>.key -> private key
// <prefix>.ca  -> CA certificate (optional)
// ca.crl       -> CA certificate revocation list (optional)
//
// If no public/private key files are found, a new key pair will be generated
// and saved on disk.
//
// If a CA certificate is found, it will be returned as well as second return
// value (otherwise it will be nil).
func KeyPairAndCA(dir, prefix string) (cowsqltls.CertInfo, error) {
	certFilename := filepath.Join(dir, prefix+".crt")
	keyFilename := filepath.Join(dir, prefix+".key")

	// Ensure that the certificate exists, or create a new one if it does
	// not.
	err := FindOrGenCert(certFilename, keyFilename, false, true)
	if err != nil {
		return nil, err
	}

	// Load the certificate.
	keypair, err := tls.LoadX509KeyPair(certFilename, keyFilename)
	if err != nil {
		return nil, err
	}

	// If available, load the CA data as well.
	caFilename := filepath.Join(dir, prefix+".ca")

	var ca *x509.Certificate
	if file.PathExists(caFilename) {
		ca, err = ReadCert(caFilename)
		if err != nil {
			return nil, err
		}
	}

	crlFilename := filepath.Join(dir, "ca.crl")

	var crl *x509.RevocationList

	if file.PathExists(crlFilename) {
		data, err := os.ReadFile(crlFilename)
		if err != nil {
			return nil, err
		}

		pemData, _ := pem.Decode(data)
		if pemData == nil {
			return nil, errors.New("Invalid revocation list")
		}

		crl, err = x509.ParseRevocationList(pemData.Bytes)
		if err != nil {
			return nil, err
		}
	}

	return cowsqltls.NewCertInfo(keypair, ca, crl), nil
}

// KeyPairFromRaw returns a CertInfo from the raw certificate and key.
func KeyPairFromRaw(certificate []byte, key []byte) (cowsqltls.CertInfo, error) {
	keypair, err := tls.X509KeyPair(certificate, key)
	if err != nil {
		return nil, err
	}

	return cowsqltls.NewCertInfo(keypair, nil, nil), nil
}

// FindOrGenCert generates a keypair if needed.
// The type argument is false for server, true for client.
func FindOrGenCert(certf string, keyf string, certtype bool, addHosts bool) error {
	if file.PathExists(certf) && file.PathExists(keyf) {
		return nil
	}

	/* If neither stat succeeded, then this is our first run and we
	 * need to generate cert and privkey */
	err := GenCert(certf, keyf, certtype, addHosts)
	if err != nil {
		return err
	}

	return nil
}

// GenCert will create and populate a certificate file and a key file.
func GenCert(certf string, keyf string, certtype bool, addHosts bool) error {
	/* Create the basenames if needed */
	dir := filepath.Dir(certf)

	err := os.MkdirAll(dir, 0o750)
	if err != nil {
		return err
	}

	dir = filepath.Dir(keyf)

	err = os.MkdirAll(dir, 0o750)
	if err != nil {
		return err
	}

	certBytes, keyBytes, err := GenerateMemCert(certtype, addHosts)
	if err != nil {
		return err
	}

	certOut, err := os.Create(certf)
	if err != nil {
		return fmt.Errorf("Failed to open %s for writing: %w", certf, err)
	}

	_, err = certOut.Write(certBytes)
	if err != nil {
		return fmt.Errorf("Failed to write cert file: %w", err)
	}

	err = certOut.Close()
	if err != nil {
		return fmt.Errorf("Failed to close cert file: %w", err)
	}

	keyOut, err := os.OpenFile(keyf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("Failed to open %s for writing: %w", keyf, err)
	}

	_, err = keyOut.Write(keyBytes)
	if err != nil {
		return fmt.Errorf("Failed to write key file: %w", err)
	}

	err = keyOut.Close()
	if err != nil {
		return fmt.Errorf("Failed to close key file: %w", err)
	}

	return nil
}

// GenerateMemCert creates client or server certificate and key pair,
// returning them as byte arrays in memory.
func GenerateMemCert(client bool, addHosts bool) ([]byte, []byte, error) {
	privk, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to generate key: %w", err)
	}

	validFrom := time.Now()
	validTo := validFrom.Add(10 * 365 * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)

	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to generate serial number: %w", err)
	}

	userEntry, err := user.Current()

	var username string
	if err == nil {
		username = userEntry.Username
		if username == "" {
			username = "UNKNOWN"
		}
	} else {
		username = "UNKNOWN"
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "UNKNOWN"
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"COWSQL"},
			CommonName:   fmt.Sprintf("%s@%s", username, hostname),
		},
		NotBefore: validFrom,
		NotAfter:  validTo,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	if client {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	if addHosts {
		hosts, err := mynames()
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to get my hostname: %w", err)
		}

		for _, h := range hosts {
			ip, _, err := net.ParseCIDR(h)
			if err == nil {
				if !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() {
					template.IPAddresses = append(template.IPAddresses, ip)
				}
			} else {
				template.DNSNames = append(template.DNSNames, h)
			}
		}
	} else if !client {
		template.DNSNames = []string{"unspecified"}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privk.PublicKey, privk)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create certificate: %w", err)
	}

	data, err := x509.MarshalECPrivateKey(privk)
	if err != nil {
		return nil, nil, err
	}

	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	key := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: data})

	return cert, key, nil
}

// ReadCert reads a PEM encoded certificate.
func ReadCert(fpath string) (*x509.Certificate, error) {
	cf, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}

	certBlock, _ := pem.Decode(cf)
	if certBlock == nil {
		return nil, errors.New("Invalid certificate file")
	}

	return x509.ParseCertificate(certBlock.Bytes)
}

/*
 * Generate a list of names for which the certificate will be valid.
 * This will include the hostname and ip address.
 */
func mynames() ([]string, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	ret := []string{h, "127.0.0.1/8", "::1/128"}

	return ret, nil
}
