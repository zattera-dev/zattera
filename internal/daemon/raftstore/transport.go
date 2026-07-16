package raftstore

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// nodeURIPrefix is the scheme+authority of a node peer certificate's URI SAN
// (zattera://node/<id>). Raft peers must present one so a stray CLI cert cannot
// open a replication stream.
const nodeURIPrefix = "zattera://node/"

// raftTLSStreamLayer is a raft.StreamLayer that wraps TCP in mutual TLS: both
// ends present the cluster node identity cert and verify the peer chains to the
// cluster CA and carries a node URI SAN. This is what makes the raft transport
// (log replication, votes, snapshots) safe to bind on the mesh IP.
type raftTLSStreamLayer struct {
	advertise net.Addr
	listener  net.Listener
	client    *tls.Config
	dialer    *net.Dialer
}

// NewTLSTransport builds a raft transport whose replication streams run over
// mTLS. nodeCert is this node's identity leaf (client + server auth); caPool
// trusts the cluster root. bindAddr is the local listen address (mesh IP:port);
// advertiseAddr is what peers dial (defaults to bindAddr).
func NewTLSTransport(bindAddr, advertiseAddr string, nodeCert tls.Certificate, caPool *x509.CertPool, logOutput io.Writer) (raft.Transport, error) {
	if advertiseAddr == "" {
		advertiseAddr = bindAddr
	}
	advertise, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("raftstore: advertise addr: %w", err)
	}
	if advertise.IP == nil || advertise.IP.IsUnspecified() {
		return nil, fmt.Errorf("raftstore: advertise addr %q must be a concrete IP (raft peers dial it)", advertiseAddr)
	}

	verify := verifyNodePeer(caPool)
	serverTLS := &tls.Config{
		MinVersion:            tls.VersionTLS12,
		Certificates:          []tls.Certificate{nodeCert},
		ClientCAs:             caPool,
		ClientAuth:            tls.RequireAndVerifyClientCert,
		VerifyPeerCertificate: verify,
	}
	// The dial target is an IP; skip hostname verification and rely on the
	// chain + URI-SAN check in VerifyPeerCertificate instead (node certs carry
	// the mesh IP, but pinning to it is redundant given the CA-signed URI SAN).
	clientTLS := &tls.Config{
		MinVersion:            tls.VersionTLS12,
		Certificates:          []tls.Certificate{nodeCert},
		RootCAs:               caPool,
		InsecureSkipVerify:    true, //nolint:gosec // replaced by verifyNodePeer below
		VerifyPeerCertificate: verify,
	}

	lis, err := tls.Listen("tcp", bindAddr, serverTLS)
	if err != nil {
		return nil, fmt.Errorf("raftstore: tls listen %s: %w", bindAddr, err)
	}
	sl := &raftTLSStreamLayer{
		advertise: advertise,
		listener:  lis,
		client:    clientTLS,
		dialer:    &net.Dialer{},
	}
	return raft.NewNetworkTransport(sl, 3, 10*time.Second, logOutput), nil
}

// Dial opens an outgoing mTLS connection to a peer.
func (s *raftTLSStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := *s.dialer
	d.Timeout = timeout
	return tls.DialWithDialer(&d, "tcp", string(address), s.client)
}

// Accept returns the next inbound mTLS connection.
func (s *raftTLSStreamLayer) Accept() (net.Conn, error) { return s.listener.Accept() }

// Close stops accepting.
func (s *raftTLSStreamLayer) Close() error { return s.listener.Close() }

// Addr is the address peers dial.
func (s *raftTLSStreamLayer) Addr() net.Addr { return s.advertise }

// verifyNodePeer returns a tls VerifyPeerCertificate hook that (independent of
// hostname) verifies the presented leaf chains to the cluster CA and carries a
// node URI SAN. Used on both ends so only cluster nodes can join the transport.
func verifyNodePeer(caPool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("raftstore: peer presented no certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("raftstore: parse peer cert: %w", err)
		}
		intermediates := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			if c, err := x509.ParseCertificate(raw); err == nil {
				intermediates.AddCert(c)
			}
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         caPool,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		}); err != nil {
			return fmt.Errorf("raftstore: peer cert not trusted: %w", err)
		}
		for _, u := range leaf.URIs {
			if strings.HasPrefix(u.String(), nodeURIPrefix) {
				return nil
			}
		}
		return fmt.Errorf("raftstore: peer cert lacks a node URI SAN")
	}
}
