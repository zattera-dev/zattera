// Package ca implements the embedded cluster certificate authority. A single
// ECDSA P-256 root, persisted under <data-dir>/ca/, signs the TLS certificates
// for the API/registry listeners and the per-node client+server certs that
// carry each node's identity in a URI SAN (zattera://node/<nodeID>). The API's
// auth layer reads that URI SAN to establish node identity from mTLS.
//
// The root key is never regenerated: if it exists but fails to parse we fail
// loudly rather than mint a new CA, because a fresh CA silently invalidates
// every node's trust and every issued cert.
package ca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const (
	rootCommonName = "zattera-cluster-ca"
	rootValidity   = 10 * 365 * 24 * time.Hour // 10 years
	// NodeCertTTL is the default validity of a node identity cert (1 year).
	NodeCertTTL = 365 * 24 * time.Hour
	nodeURISAN  = "zattera://node/"
)

// CA is the cluster certificate authority: an ECDSA P-256 root that issues
// leaf certs. It is safe for concurrent use (its fields are read-only after
// construction).
type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// pool trusts the root, for verifying issued/presented certs.
	pool *x509.CertPool
	// certPEM is the root certificate in PEM form (the CA bundle).
	certPEM []byte
}

// LoadOrCreate loads the root CA from <dir>/ca.crt + ca.key, creating a fresh
// root (and persisting it 0600) if none exists. A present-but-unparseable key
// is a hard error — never silently regenerate.
func LoadOrCreate(dir string) (*CA, error) {
	caDir := filepath.Join(dir, "ca")
	certPath := filepath.Join(caDir, "ca.crt")
	keyPath := filepath.Join(caDir, "ca.key")

	certPEM, certErr := os.ReadFile(certPath)
	keyPEM, keyErr := os.ReadFile(keyPath)
	switch {
	case certErr == nil && keyErr == nil:
		return loadCA(certPEM, keyPEM)
	case os.IsNotExist(certErr) && os.IsNotExist(keyErr):
		return createCA(caDir, certPath, keyPath)
	default:
		// One of the two exists but not the other, or an unexpected read error.
		// Regenerating would brick trust — fail loudly.
		return nil, fmt.Errorf("ca: inconsistent CA material in %s (cert err: %v, key err: %v)", caDir, certErr, keyErr)
	}
}

func loadCA(certPEM, keyPEM []byte) (*CA, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("ca: ca.crt is not a PEM certificate")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ca: parse ca.crt: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil || keyBlock.Type != "EC PRIVATE KEY" {
		return nil, fmt.Errorf("ca: ca.key is not a PEM EC private key")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ca: parse ca.key: %w", err)
	}
	return newCA(cert, key, certPEM), nil
}

func createCA(caDir, certPath, keyPath string) (*CA, error) {
	if err := os.MkdirAll(caDir, 0o700); err != nil {
		return nil, fmt.Errorf("ca: create dir: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ca: generate root key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: rootCommonName},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(rootValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("ca: create root cert: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("ca: parse root cert: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("ca: marshal root key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		return nil, fmt.Errorf("ca: write ca.crt: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return nil, fmt.Errorf("ca: write ca.key: %w", err)
	}
	return newCA(cert, key, certPEM), nil
}

func newCA(cert *x509.Certificate, key *ecdsa.PrivateKey, certPEM []byte) *CA {
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{cert: cert, key: key, pool: pool, certPEM: certPEM}
}

// Leaf is one issued certificate + its private key, in PEM.
type Leaf struct {
	CertPEM []byte
	KeyPEM  []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

// TLSCertificate builds a tls.Certificate from an issued leaf, with the CA
// appended so peers can build the chain.
func (l *Leaf) TLSCertificate(caPEM []byte) (tls.Certificate, error) {
	full := append(append([]byte(nil), l.CertPEM...), caPEM...)
	return tls.X509KeyPair(full, l.KeyPEM)
}

func (c *CA) issue(tmpl *x509.Certificate) (*Leaf, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ca: leaf key: %w", err)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		return nil, fmt.Errorf("ca: sign leaf: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return &Leaf{CertPEM: certPEM, KeyPEM: keyPEM, cert: cert, key: key}, nil
}

// IssueServer mints a server cert for the API/registry listeners. Callers
// should include 127.0.0.1, the node's mesh IP, the cluster domain and
// localhost among dnsNames/ips.
func (c *CA) IssueServer(dnsNames []string, ips []net.IP, ttl time.Duration) (*Leaf, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: firstOr(dnsNames, "zattera-server")},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	return c.issue(tmpl)
}

// IssueNode mints a node identity cert used both as a client and a server
// cert (nodes speak mTLS both ways). Its identity is carried in a URI SAN
// zattera://node/<nodeID> plus DNS SAN node-<nodeID>.
func (c *CA) IssueNode(nodeID string, meshIP net.IP, ttl time.Duration) (*Leaf, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	uri, err := url.Parse(nodeURISAN + nodeID)
	if err != nil {
		return nil, fmt.Errorf("ca: node uri san: %w", err)
	}
	var ips []net.IP
	if meshIP != nil {
		ips = []net.IP{meshIP}
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "node-" + nodeID},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"node-" + nodeID},
		IPAddresses:  ips,
		URIs:         []*url.URL{uri},
	}
	return c.issue(tmpl)
}

// SignCSR verifies a CSR's self-signature, ignores any SANs it requested, and
// issues a node identity cert with SANs we impose (the join flow, T-17).
func (c *CA) SignCSR(csrPEM []byte, nodeID string, meshIP net.IP, ttl time.Duration) ([]byte, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, fmt.Errorf("ca: not a PEM certificate request")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ca: parse csr: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("ca: csr signature: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	uri, err := url.Parse(nodeURISAN + nodeID)
	if err != nil {
		return nil, fmt.Errorf("ca: node uri san: %w", err)
	}
	var ips []net.IP
	if meshIP != nil {
		ips = []net.IP{meshIP}
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:       serial,
		Subject:            pkix.Name{CommonName: "node-" + nodeID},
		NotBefore:          now.Add(-5 * time.Minute),
		NotAfter:           now.Add(ttl),
		KeyUsage:           x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:        []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:           []string{"node-" + nodeID},
		IPAddresses:        ips,
		URIs:               []*url.URL{uri},
		PublicKeyAlgorithm: csr.PublicKeyAlgorithm,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, csr.PublicKey, c.key)
	if err != nil {
		return nil, fmt.Errorf("ca: sign csr cert: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// CABundlePEM returns the root certificate in PEM (the trust bundle handed to
// joining nodes and CLI clients).
func (c *CA) CABundlePEM() []byte {
	return append([]byte(nil), c.certPEM...)
}

// PrivateKeyPEM returns the root CA private key in PEM (EC PRIVATE KEY). It is
// handed to a joining control node over the mTLS join hop (T-55) so every
// control node can independently sign node certs, serve its API cert, and run
// ACME. Guard it like any cluster secret — a leak is a full trust compromise.
func (c *CA) PrivateKeyPEM() ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(c.key)
	if err != nil {
		return nil, fmt.Errorf("ca: marshal root key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}

// Certificate returns the parsed root certificate (used for the CA-pin hash in
// join tokens, T-12).
func (c *CA) Certificate() *x509.Certificate { return c.cert }

// Pool returns a cert pool trusting the root.
func (c *CA) Pool() *x509.CertPool { return c.pool.Clone() }

// ServerTLSConfig returns a *tls.Config presenting a freshly issued server
// cert for the given SANs, requesting (not requiring) client certs so both
// token-bearing CLIs and mTLS nodes can share the listener.
func (c *CA) ServerTLSConfig(dnsNames []string, ips []net.IP) (*tls.Config, error) {
	l, err := c.IssueServer(dnsNames, ips, NodeCertTTL)
	if err != nil {
		return nil, err
	}
	tlsCert, err := l.TLSCertificate(c.certPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    c.pool.Clone(),
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}, nil
}

// ClientTLSConfig returns a *tls.Config presenting nodeCert (a node identity
// leaf) and trusting the cluster root — for node→control mTLS dials.
func (c *CA) ClientTLSConfig(nodeCert tls.Certificate) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{nodeCert},
		RootCAs:      c.pool.Clone(),
	}
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("ca: serial: %w", err)
	}
	return serial, nil
}

func firstOr(ss []string, fallback string) string {
	if len(ss) > 0 && ss[0] != "" {
		return ss[0]
	}
	return fallback
}
