// Package testcerts generates a throw-away PKI (CA, server and client
// certificates) so TLS/mTLS behaviour can be tested without checked-in secrets.
package testcerts

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Bundle is one CA with a server and a client certificate it issued. File paths
// point at PEM files written into the directory given to New.
type Bundle struct {
	CAPath         string
	ServerCertPath string
	ServerKeyPath  string
	ClientCertPath string
	ClientKeyPath  string

	CAPool     *x509.CertPool
	ServerCert tls.Certificate
	ClientCert tls.Certificate
}

type issued struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
	keyPEM  []byte
}

// New creates a bundle. The server certificate is valid for "localhost" and
// 127.0.0.1. Call it twice to get two unrelated CAs (an "untrusted" peer).
func New(t testing.TB, dir string) *Bundle {
	t.Helper()
	ca := issue(t, nil, "test-ca", true, nil)
	server := issue(t, ca, "server", false, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	client := issue(t, ca, "client", false, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)

	serverPair, err := tls.X509KeyPair(server.certPEM, server.keyPEM)
	if err != nil {
		t.Fatalf("testcerts: server pair: %v", err)
	}
	clientPair, err := tls.X509KeyPair(client.certPEM, client.keyPEM)
	if err != nil {
		t.Fatalf("testcerts: client pair: %v", err)
	}

	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatalf("testcerts: write %s: %v", name, err)
		}
		return p
	}
	return &Bundle{
		CAPath:         write("ca.pem", ca.certPEM),
		ServerCertPath: write("server.pem", server.certPEM),
		ServerKeyPath:  write("server.key", server.keyPEM),
		ClientCertPath: write("client.pem", client.certPEM),
		ClientKeyPath:  write("client.key", client.keyPEM),
		CAPool:         pool,
		ServerCert:     serverPair,
		ClientCert:     clientPair,
	}
}

// ServerConfig returns a tls.Config for a test server presenting the bundle's
// server certificate; with requireClient it demands a client certificate signed
// by the same CA.
func (b *Bundle) ServerConfig(requireClient bool) *tls.Config {
	cfg := &tls.Config{Certificates: []tls.Certificate{b.ServerCert}, MinVersion: tls.VersionTLS12}
	if requireClient {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = b.CAPool
	}
	return cfg
}

func issue(t testing.TB, parent *issued, cn string, isCA bool, usage []x509.ExtKeyUsage) *issued {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testcerts: key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("testcerts: serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           usage,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		tmpl.KeyUsage |= x509.KeyUsageCertSign
	} else {
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatalf("testcerts: certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testcerts: parse: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("testcerts: marshal key: %v", err)
	}
	return &issued{
		cert: cert, key: key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}
