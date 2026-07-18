package api

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/testutil/simcluster"
)

// startForwardAPI stands up an API server for one raft node with the leader-
// forward interceptor outermost. resolve returns the leader's API address.
func startForwardAPI(t *testing.T, rs *raftstore.Store, authority *ca.CA, dialOpts []grpc.DialOption, resolve func() (string, error)) *Server {
	t.Helper()
	st := rs.State()
	clk := clock.Real{}
	auth := NewAuthenticator(st, rs, clk)
	rbac := NewRBAC(st)
	fwd := NewLeaderForwarder(rs.IsLeader, resolve, dialOpts, nil)

	srv, err := New(Options{
		CA:             authority,
		Listen:         "127.0.0.1:0",
		DNSNames:       []string{"localhost"},
		IPs:            []net.IP{net.ParseIP("127.0.0.1")},
		AuthService:    NewAuthServer(st, rs, clk, "", secrets.NewVault()),
		ProjectService: NewProjectServer(st, rs, clk, rbac),
		UnaryInterceptors: []grpc.UnaryServerInterceptor{
			fwd.UnaryInterceptor, auth.UnaryInterceptor, rbac.UnaryInterceptor,
		},
		StreamInterceptors: []grpc.StreamServerInterceptor{auth.StreamInterceptor},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx) }()
	t.Cleanup(cancel)
	waitReady(t, srv.Addr().String(), authority.Pool())
	return srv
}

func TestLeaderForward(t *testing.T) {
	c := simcluster.New(t, 3)
	leader := c.Leader()
	var follower *simcluster.Node
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}

	// Seed org + admin user + token on the leader (replicates to followers).
	clk := clock.Real{}
	uid := ids.New()
	token, hashTok, _ := MintToken()
	for _, cmd := range []*clusterv1.Command{
		{Mutation: &clusterv1.Command_PutOrg{PutOrg: &clusterv1.PutOrg{Org: &zatterav1.Org{Meta: newMeta(ids.New(), clk.Now()), Name: "default"}}}},
		{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{User: &zatterav1.User{Meta: newMeta(uid, clk.Now()), Email: "admin@local", OrgRole: zatterav1.Role_ROLE_ADMIN}}}},
		{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{Token: &zatterav1.Token{Meta: newMeta(ids.New(), clk.Now()), UserId: uid, SecretHash: hashTok}}}},
	} {
		cmd.RequestId = ids.New()
		if err := c.Apply(cmd); err != nil {
			t.Fatalf("seed apply: %v", err)
		}
	}

	// One CA shared by both servers so the follower can dial the leader and the
	// client can trust both.
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tlsCreds := credentials.NewTLS(&tls.Config{RootCAs: authority.Pool(), ServerName: "127.0.0.1"})
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	// Leader API (leader-forward is a no-op here).
	leaderSrv := startForwardAPI(t, leader.Store, authority, dialOpts, func() (string, error) { return "", nil })
	leaderAddr := leaderSrv.Addr().String()

	// Follower API: resolve points at the leader's API address.
	followerSrv := startForwardAPI(t, follower.Store, authority, dialOpts, func() (string, error) {
		return leaderAddr, nil
	})

	// Client talks to the FOLLOWER; the mutation must be forwarded to the leader.
	conn, err := grpc.NewClient(followerSrv.Addr().String(), grpc.WithTransportCredentials(tlsCreds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	pc := zatterav1.NewProjectServiceClient(conn)

	// Wait for the seeded token to replicate to the follower is NOT needed: the
	// forwarded call is authenticated on the leader. Create a project.
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
	proj, err := pc.CreateProject(ctx, &zatterav1.CreateProjectRequest{Name: "forwarded"})
	if err != nil {
		t.Fatalf("forwarded CreateProject: %v", err)
	}
	if proj.GetName() != "forwarded" {
		t.Fatalf("name = %q", proj.GetName())
	}

	// It must exist in the LEADER's state (that's where the apply landed).
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := leader.State.ProjectByName("forwarded"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("forwarded project never appeared in leader state")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Loop guard: a request already marked forwarded must NOT be forwarded again
	// by a follower.
	loopCtx := metadata.AppendToOutgoingContext(ctx, forwardedMDKey, "1")
	_, err = pc.CreateProject(loopCtx, &zatterav1.CreateProjectRequest{Name: "loop"})
	if status.Code(err) != codes.Internal {
		t.Errorf("loop-guard code = %v, want Internal", status.Code(err))
	}
}

func TestForwardedAlready(t *testing.T) {
	// no marker → false
	if forwardedAlready(context.Background()) {
		t.Error("empty ctx should not look forwarded")
	}
	md := metadata.Pairs(forwardedMDKey, "1")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	if !forwardedAlready(ctx) {
		t.Error("marked ctx should look forwarded")
	}
}

func TestNewReplyMessage(t *testing.T) {
	m, err := newReplyMessage("/zattera.v1.ProjectService/CreateProject")
	if err != nil {
		t.Fatalf("newReplyMessage: %v", err)
	}
	if got := string(m.ProtoReflect().Descriptor().FullName()); got != "zattera.v1.Project" {
		t.Errorf("reply type = %s, want zattera.v1.Project", got)
	}
	if _, err := newReplyMessage("/no.such.Service/Method"); err == nil {
		t.Error("expected error for unknown service")
	}
}
