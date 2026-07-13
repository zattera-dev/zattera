// Package apiclient is the public Go client for the Zattera API. The CLI is
// built on it; third parties may use it directly. It wraps a single gRPC
// connection with bearer-token auth and typed service accessors.
package apiclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// Config for a client connection.
type Config struct {
	// Address like "paas.example.com:8443" (scheme optional, https implied).
	Address string
	Token   string
	// CACertPEM pins the cluster CA. Empty = system roots.
	CACertPEM []byte
	// InsecureSkipVerify is for `--dev` servers with self-signed certs whose
	// CA the CLI has not stored (discouraged; prefer CACertPEM).
	InsecureSkipVerify bool
}

// Client is a connected API client.
type Client struct {
	conn *grpc.ClientConn

	Auth     zatterav1.AuthServiceClient
	Projects zatterav1.ProjectServiceClient
	Apps     zatterav1.AppServiceClient
	Deploys  zatterav1.DeployServiceClient
	Nodes    zatterav1.NodeServiceClient
	Logs     zatterav1.LogServiceClient
	Metrics  zatterav1.MetricsServiceClient
	Domains  zatterav1.DomainServiceClient
	Volumes  zatterav1.VolumeServiceClient
	Jobs     zatterav1.JobServiceClient
	Exec     zatterav1.ExecServiceClient
	State    zatterav1.StateServiceClient
	Alerts   zatterav1.AlertServiceClient
	Audit    zatterav1.AuditServiceClient
	Backup   zatterav1.BackupServiceClient
}

// New dials the API. The connection is lazy; the first RPC connects.
func New(cfg Config) (*Client, error) {
	addr := strings.TrimPrefix(strings.TrimPrefix(cfg.Address, "https://"), "grpc://")
	if addr == "" {
		return nil, fmt.Errorf("apiclient: address is required")
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(cfg.CACertPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.CACertPEM) {
			return nil, fmt.Errorf("apiclient: invalid CA certificate")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.InsecureSkipVerify {
		tlsCfg.InsecureSkipVerify = true
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithPerRPCCredentials(bearerCreds{token: cfg.Token}),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:     conn,
		Auth:     zatterav1.NewAuthServiceClient(conn),
		Projects: zatterav1.NewProjectServiceClient(conn),
		Apps:     zatterav1.NewAppServiceClient(conn),
		Deploys:  zatterav1.NewDeployServiceClient(conn),
		Nodes:    zatterav1.NewNodeServiceClient(conn),
		Logs:     zatterav1.NewLogServiceClient(conn),
		Metrics:  zatterav1.NewMetricsServiceClient(conn),
		Domains:  zatterav1.NewDomainServiceClient(conn),
		Volumes:  zatterav1.NewVolumeServiceClient(conn),
		Jobs:     zatterav1.NewJobServiceClient(conn),
		Exec:     zatterav1.NewExecServiceClient(conn),
		State:    zatterav1.NewStateServiceClient(conn),
		Alerts:   zatterav1.NewAlertServiceClient(conn),
		Audit:    zatterav1.NewAuditServiceClient(conn),
		Backup:   zatterav1.NewBackupServiceClient(conn),
	}, nil
}

// Close tears down the connection.
func (c *Client) Close() error { return c.conn.Close() }

type bearerCreds struct{ token string }

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if b.token == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

// RequireTransportSecurity: tokens never travel in plaintext.
func (bearerCreds) RequireTransportSecurity() bool { return true }
