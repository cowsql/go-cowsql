package tls

import (
	"crypto/sha256"
	"crypto/x509"
	"database/sql/driver"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
)

// Certificate parsing type.
type Certificate struct { //nolint:recvcheck
	Certificate *x509.Certificate `json:"-" yaml:"-"`
}

// UnmarshalYAML is the certificate yaml unmarshaler.
func (c *Certificate) UnmarshalYAML(unmarshal func(v any) error) error {
	var certStr string

	err := unmarshal(&certStr)
	if err != nil {
		return err
	}

	if certStr == "" {
		*c = Certificate{}

		return nil
	}

	*c, err = CertDecodeFromPEM([]byte(certStr))
	if err != nil {
		return err
	}

	return nil
}

// MarshalYAML is the certificate yaml marshaler.
func (c Certificate) MarshalYAML() (any, error) {
	return c.String(), nil
}

// UnmarshalJSON is the certificate json unmarshaler.
func (c *Certificate) UnmarshalJSON(b []byte) error {
	var certStr string

	err := json.Unmarshal(b, &certStr)
	if err != nil {
		return err
	}

	if certStr == "" {
		*c = Certificate{}

		return nil
	}

	*c, err = CertDecodeFromPEM([]byte(certStr))
	if err != nil {
		return err
	}

	return nil
}

// MarshalJSON is the certificate json marshaler.
func (c Certificate) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// Value implements the [driver.Valuer] interface.
func (c Certificate) Value() (driver.Value, error) {
	if c == (Certificate{}) {
		return nil, nil //nolint:nilnil
	}

	return c.MarshalJSON()
}

// Scan implements the [Scanner] interface.
func (c *Certificate) Scan(value any) error {
	switch t := value.(type) {
	case nil:
		*c = Certificate{}

		return nil
	case []byte:
		return c.UnmarshalJSON(t)
	case string:
		return c.UnmarshalJSON([]byte(t))
	default:
		return fmt.Errorf("Unsupported scan type %T", t)
	}
}

// String prints the encoded certificate.
func (c *Certificate) String() string {
	if c != nil && *c != (Certificate{}) {
		return CertEncodeToPEM(*c)
	}

	return ""
}

// Fingerprint returns the hex encoded fingerprint.
func (c *Certificate) Fingerprint() (string, error) {
	if c.Certificate != nil {
		return fmt.Sprintf("%x", sha256.Sum256(c.Certificate.Raw)), nil
	}

	return "", errors.New("Certificate is invalid")
}

// CertEncodeToPEM converts a raw cert to a string, or returns an empty string for invalid certs.
func CertEncodeToPEM(data Certificate) string {
	if data.Certificate != nil {
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: data.Certificate.Raw}))
	}

	// Unknown data, assume no certificate.
	return ""
}

// CertDecodeFromPEM decodes a PEM certificate.
func CertDecodeFromPEM(certBytes []byte) (Certificate, error) {
	certBlock, _ := pem.Decode(certBytes)
	if certBlock == nil {
		return Certificate{}, errors.New("Certificate must be base64 encoded PEM certificate")
	}

	switch certBlock.Type {
	case "CERTIFICATE":
		cert, err := x509.ParseCertificate(certBlock.Bytes)
		if err != nil {
			return Certificate{}, fmt.Errorf("Failed to parse x509 certificate: %w", err)
		}

		return Certificate{Certificate: cert}, nil
	default:
		return Certificate{}, fmt.Errorf("Unsupported certificate type: %q", certBlock.Type)
	}
}
