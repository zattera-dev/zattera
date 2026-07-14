package api

import (
	"context"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// hostnameLabel matches one DNS label (RFC-952/1123-ish): 1-63 chars, no
// leading/trailing hyphen.
var hostnameLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// DomainServer implements DomainService: custom hostnames mapped to an app's
// environment, with per-domain middleware. Implicit cluster subdomains
// (<app>-<env>.<cluster-domain>) are emitted by the route builder (T-39); this
// service rejects manual hostnames that collide with that namespace.
type DomainServer struct {
	zatterav1.UnimplementedDomainServiceServer
	store         *state.Store
	raft          Applier
	clock         clock.Clock
	clusterDomain string
}

// NewDomainServer builds the domain service. clusterDomain is cfg.Domain (the
// reserved auto-subdomain suffix; empty disables the collision check).
func NewDomainServer(store *state.Store, raft Applier, clk clock.Clock, clusterDomain string) *DomainServer {
	return &DomainServer{store: store, raft: raft, clock: clk, clusterDomain: strings.ToLower(clusterDomain)}
}

// AddDomain validates and registers a hostname for an environment.
func (s *DomainServer) AddDomain(ctx context.Context, req *zatterav1.AddDomainRequest) (*zatterav1.Domain, error) {
	env, ok := s.store.Environment(req.GetEnvironmentId())
	if !ok || env.GetProjectId() != req.GetProjectId() {
		return nil, status.Error(codes.NotFound, "environment not found in this project")
	}
	host := normalizeHostname(req.GetHostname())
	if !validHostname(host) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid hostname %q", req.GetHostname())
	}
	if s.collidesWithClusterDomain(host) {
		return nil, status.Errorf(codes.InvalidArgument, "hostname %q is in the reserved cluster subdomain namespace", host)
	}
	if _, exists := s.store.DomainByHostname(host); exists {
		return nil, status.Errorf(codes.AlreadyExists, "hostname %q is already in use", host)
	}

	dom := &zatterav1.Domain{
		Meta:          newMeta(ids.New(), s.clock.Now()),
		ProjectId:     req.GetProjectId(),
		AppId:         env.GetAppId(),
		EnvironmentId: env.GetMeta().GetId(),
		Hostname:      host,
		PathPrefix:    req.GetPathPrefix(),
		PortName:      req.GetPortName(),
		CertStatus:    zatterav1.CertStatus_CERT_STATUS_PENDING,
	}
	if err := s.put(ctx, dom); err != nil {
		return nil, err
	}
	return dom, nil
}

// ListDomains lists a project's domains.
func (s *DomainServer) ListDomains(_ context.Context, req *zatterav1.ListDomainsRequest) (*zatterav1.ListDomainsResponse, error) {
	return &zatterav1.ListDomainsResponse{Domains: s.store.ListDomains(req.GetProjectId())}, nil
}

// RemoveDomain deletes a domain.
func (s *DomainServer) RemoveDomain(ctx context.Context, req *zatterav1.RemoveDomainRequest) (*emptypb.Empty, error) {
	dom, ok := s.store.Domain(req.GetDomainId())
	if !ok || dom.GetProjectId() != req.GetProjectId() {
		return nil, status.Error(codes.NotFound, "domain not found")
	}
	err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteDomain{DeleteDomain: &clusterv1.DeleteByID{Id: dom.GetMeta().GetId()}}})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// SetMiddleware replaces a domain's middleware.
func (s *DomainServer) SetMiddleware(ctx context.Context, req *zatterav1.SetMiddlewareRequest) (*zatterav1.Domain, error) {
	dom, ok := s.store.Domain(req.GetDomainId())
	if !ok || dom.GetProjectId() != req.GetProjectId() {
		return nil, status.Error(codes.NotFound, "domain not found")
	}
	dom = proto.Clone(dom).(*zatterav1.Domain)
	dom.Middleware = req.GetMiddleware()
	if err := s.put(ctx, dom); err != nil {
		return nil, err
	}
	return dom, nil
}

// SetCertStatus updates a hostname's certificate status (best-effort callback
// from the TLS manager: PENDING → ISSUED/FAILED). Silently no-ops for unknown
// or implicit hostnames.
func (s *DomainServer) SetCertStatus(ctx context.Context, hostname string, cs zatterav1.CertStatus) {
	dom, ok := s.store.DomainByHostname(normalizeHostname(hostname))
	if !ok || dom.GetCertStatus() == cs {
		return
	}
	dom = proto.Clone(dom).(*zatterav1.Domain)
	dom.CertStatus = cs
	_ = s.put(ctx, dom)
}

func (s *DomainServer) collidesWithClusterDomain(host string) bool {
	if s.clusterDomain == "" {
		return false
	}
	return host == s.clusterDomain || strings.HasSuffix(host, "."+s.clusterDomain)
}

func (s *DomainServer) put(ctx context.Context, dom *zatterav1.Domain) error {
	return s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutDomain{PutDomain: &clusterv1.PutDomain{Domain: dom}}})
}

func (s *DomainServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	id, _ := IdentityFrom(ctx)
	cmd.RequestId = ids.New()
	cmd.Actor = id.Actor()
	cmd.Time = timestamppb.Now()
	return toStatus(s.raft.Apply(ctx, cmd))
}

// normalizeHostname lowercases and strips a trailing dot.
func normalizeHostname(h string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(h), "."))
}

// validHostname reports whether h is a syntactically valid DNS hostname.
func validHostname(h string) bool {
	if h == "" || len(h) > 253 {
		return false
	}
	for _, label := range strings.Split(h, ".") {
		if !hostnameLabel.MatchString(label) {
			return false
		}
	}
	return true
}
