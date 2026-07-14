package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func newDomainServer(t *testing.T, clusterDomain string) (*DomainServer, *raftstore.Store) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app"}, ProjectId: "proj", Name: "api"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env"}, ProjectId: "proj", AppId: "app", Name: "production"})
	return NewDomainServer(st, rs, clock.NewFake(), clusterDomain), rs
}

func addReq(host string) *zatterav1.AddDomainRequest {
	return &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "env", Hostname: host}
}

func TestDomainsCRUD(t *testing.T) {
	s, _ := newDomainServer(t, "apps.example.com")
	ctx := context.Background()

	dom, err := s.AddDomain(ctx, addReq("API.Example.com")) // mixed case → normalized
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if dom.GetHostname() != "api.example.com" {
		t.Fatalf("hostname not normalized: %q", dom.GetHostname())
	}
	if dom.GetAppId() != "app" || dom.GetEnvironmentId() != "env" {
		t.Fatalf("domain not linked to env/app: %+v", dom)
	}
	if dom.GetCertStatus() != zatterav1.CertStatus_CERT_STATUS_PENDING {
		t.Fatalf("cert status = %v, want PENDING", dom.GetCertStatus())
	}

	list, _ := s.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: "proj"})
	if len(list.GetDomains()) != 1 {
		t.Fatalf("list = %d domains", len(list.GetDomains()))
	}

	if _, err := s.RemoveDomain(ctx, &zatterav1.RemoveDomainRequest{ProjectId: "proj", DomainId: dom.GetMeta().GetId()}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	list, _ = s.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: "proj"})
	if len(list.GetDomains()) != 0 {
		t.Fatal("domain not removed")
	}
}

func TestDomainsValidationAndUniqueness(t *testing.T) {
	s, _ := newDomainServer(t, "apps.example.com")
	ctx := context.Background()

	// Invalid hostname.
	if _, err := s.AddDomain(ctx, addReq("not a host!")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid host code = %v, want InvalidArgument", status.Code(err))
	}
	// Unknown environment.
	if _, err := s.AddDomain(ctx, &zatterav1.AddDomainRequest{ProjectId: "proj", EnvironmentId: "ghost", Hostname: "x.example.com"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown env code = %v, want NotFound", status.Code(err))
	}
	// Duplicate hostname.
	if _, err := s.AddDomain(ctx, addReq("dup.example.com")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddDomain(ctx, addReq("dup.example.com")); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestDomainsClusterSubdomainCollision(t *testing.T) {
	s, _ := newDomainServer(t, "apps.example.com")
	ctx := context.Background()

	// A hostname under the reserved cluster domain is rejected.
	if _, err := s.AddDomain(ctx, addReq("api-production.apps.example.com")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cluster-subdomain collision code = %v, want InvalidArgument", status.Code(err))
	}
	// The cluster apex itself is rejected too.
	if _, err := s.AddDomain(ctx, addReq("apps.example.com")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("apex collision code = %v, want InvalidArgument", status.Code(err))
	}
	// A hostname outside the cluster domain is fine.
	if _, err := s.AddDomain(ctx, addReq("api.example.com")); err != nil {
		t.Fatalf("external hostname rejected: %v", err)
	}
}

func TestDomainsSetMiddlewareAndCertStatus(t *testing.T) {
	s, _ := newDomainServer(t, "")
	ctx := context.Background()

	dom, err := s.AddDomain(ctx, addReq("api.example.com"))
	if err != nil {
		t.Fatal(err)
	}
	mw := &zatterav1.Middleware{Compress: true, MaxBodyBytes: 1024}
	updated, err := s.SetMiddleware(ctx, &zatterav1.SetMiddlewareRequest{ProjectId: "proj", DomainId: dom.GetMeta().GetId(), Middleware: mw})
	if err != nil {
		t.Fatal(err)
	}
	if !updated.GetMiddleware().GetCompress() || updated.GetMiddleware().GetMaxBodyBytes() != 1024 {
		t.Fatalf("middleware not applied: %+v", updated.GetMiddleware())
	}

	// The cert-status callback promotes PENDING → ISSUED.
	s.SetCertStatus(ctx, "api.example.com", zatterav1.CertStatus_CERT_STATUS_ISSUED)
	got, _ := s.store.Domain(dom.GetMeta().GetId())
	if got.GetCertStatus() != zatterav1.CertStatus_CERT_STATUS_ISSUED {
		t.Fatalf("cert status = %v, want ISSUED", got.GetCertStatus())
	}
}
