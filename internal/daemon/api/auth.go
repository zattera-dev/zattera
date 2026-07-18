package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/argon2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// sessionTTL is the lifetime of a Login-issued token.
const sessionTTL = 24 * time.Hour

// AuthServer implements zatterav1.AuthServiceServer.
type AuthServer struct {
	zatterav1.UnimplementedAuthServiceServer
	store         *state.Store
	raft          Applier
	clock         clock.Clock
	clusterDomain string // cfg.Domain; surfaced via WhoAmI for app-URL construction
	vault         *secrets.Vault
	// onUnseal, when set, persists the recovered key so the next restart is
	// automatic. Nil when the operator chose sealed-at-rest.
	onUnseal func()
}

// NewAuthServer builds the auth service. clusterDomain is cfg.Domain; vault is
// this node's cluster key holder, which Unseal installs into.
func NewAuthServer(store *state.Store, raft Applier, clk clock.Clock, clusterDomain string, vault *secrets.Vault) *AuthServer {
	return &AuthServer{store: store, raft: raft, clock: clk, clusterDomain: clusterDomain, vault: vault}
}

// SetUnsealHook registers a callback run after a successful Unseal.
func (s *AuthServer) SetUnsealHook(fn func()) { s.onUnseal = fn }

// Login exchanges email+password for a short-lived session token.
func (s *AuthServer) Login(ctx context.Context, req *zatterav1.LoginRequest) (*zatterav1.LoginResponse, error) {
	user, ok := s.store.UserByEmail(req.GetEmail())
	if !ok || !verifyPassword(user.GetPasswordHash(), req.GetPassword()) {
		return nil, status.Error(codes.Unauthenticated, "invalid email or password")
	}
	token, secretHash, err := MintToken()
	if err != nil {
		return nil, status.Error(codes.Internal, "token generation failed")
	}
	now := s.clock.Now()
	tok := &zatterav1.Token{
		Meta:       newMeta(ids.New(), now),
		UserId:     user.GetMeta().GetId(),
		Name:       "session",
		SecretHash: secretHash,
		Kind:       zatterav1.TokenKind_TOKEN_KIND_SESSION,
		ExpiresAt:  timestamppb.New(now.Add(sessionTTL)),
	}
	if err := s.apply(ctx, user.GetMeta().GetId(), &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{Token: tok}}}); err != nil {
		return nil, toStatus(err)
	}
	return &zatterav1.LoginResponse{Token: token, User: redactUser(user)}, nil
}

// WhoAmI returns the caller's user and project memberships.
func (s *AuthServer) WhoAmI(ctx context.Context, _ *emptypb.Empty) (*zatterav1.WhoAmIResponse, error) {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "no user identity")
	}
	user, ok := s.store.User(id.UserID)
	if !ok {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return &zatterav1.WhoAmIResponse{
		User:          redactUser(user),
		Memberships:   s.store.ListMembershipsOfUser(id.UserID),
		ClusterDomain: s.clusterDomain,
		Sealed:        !s.vault.Unsealed(),
	}, nil
}

// Unseal installs the cluster data key on this node from the recovery
// passphrase (T-111). It is per-node: each sealed node must be unsealed, since
// the key lives in process memory and is never replicated.
func (s *AuthServer) Unseal(_ context.Context, req *zatterav1.UnsealRequest) (*zatterav1.UnsealResponse, error) {
	if s.vault.Unsealed() {
		return &zatterav1.UnsealResponse{AlreadyUnsealed: true}, nil
	}
	if req.GetPassphrase() == "" {
		return nil, status.Error(codes.InvalidArgument, "passphrase is required")
	}
	km, ok := s.store.ClusterKeyMaterial()
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "cluster key material unavailable")
	}
	if err := s.vault.UnsealWithPassphrase(km, req.GetPassphrase()); err != nil {
		if errors.Is(err, secrets.ErrSealedDataInvalid) {
			return nil, status.Error(codes.PermissionDenied, "wrong recovery passphrase")
		}
		return nil, toStatus(err)
	}
	if s.onUnseal != nil {
		s.onUnseal()
	}
	return &zatterav1.UnsealResponse{}, nil
}

// CreateToken issues a personal access token for the caller.
func (s *AuthServer) CreateToken(ctx context.Context, req *zatterav1.CreateTokenRequest) (*zatterav1.CreateTokenResponse, error) {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "no user identity")
	}
	token, secretHash, err := MintToken()
	if err != nil {
		return nil, status.Error(codes.Internal, "token generation failed")
	}
	now := s.clock.Now()
	tok := &zatterav1.Token{
		Meta:       newMeta(ids.New(), now),
		UserId:     id.UserID,
		Name:       req.GetName(),
		SecretHash: secretHash,
		Kind:       zatterav1.TokenKind_TOKEN_KIND_PERSONAL,
	}
	if d := req.GetTtl().AsDuration(); d > 0 {
		tok.ExpiresAt = timestamppb.New(now.Add(d))
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutToken{PutToken: &clusterv1.PutToken{Token: tok}}}); err != nil {
		return nil, toStatus(err)
	}
	return &zatterav1.CreateTokenResponse{Token: token, Info: redactToken(tok)}, nil
}

// ListTokens lists the caller's tokens (secret hashes redacted).
func (s *AuthServer) ListTokens(ctx context.Context, _ *emptypb.Empty) (*zatterav1.ListTokensResponse, error) {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "no user identity")
	}
	tokens := s.store.ListTokens(id.UserID)
	for i := range tokens {
		tokens[i] = redactToken(tokens[i])
	}
	return &zatterav1.ListTokensResponse{Tokens: tokens}, nil
}

// RevokeToken deletes one of the caller's tokens (admins may revoke any).
func (s *AuthServer) RevokeToken(ctx context.Context, req *zatterav1.RevokeTokenRequest) (*emptypb.Empty, error) {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "no user identity")
	}
	tok, ok := s.store.Token(req.GetTokenId())
	if !ok {
		return nil, status.Error(codes.NotFound, "token not found")
	}
	isAdmin := id.OrgRole == zatterav1.Role_ROLE_OWNER || id.OrgRole == zatterav1.Role_ROLE_ADMIN
	if tok.GetUserId() != id.UserID && !isAdmin {
		return nil, status.Error(codes.PermissionDenied, "cannot revoke another user's token")
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteToken{DeleteToken: &clusterv1.DeleteByID{Id: req.GetTokenId()}}}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// CreateUser adds a user to the org (admin only; tier enforced by the policy).
func (s *AuthServer) CreateUser(ctx context.Context, req *zatterav1.CreateUserRequest) (*zatterav1.User, error) {
	if req.GetEmail() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}
	if _, exists := s.store.UserByEmail(req.GetEmail()); exists {
		return nil, status.Errorf(codes.AlreadyExists, "user %s already exists", req.GetEmail())
	}
	hash, err := hashPassword(req.GetPassword())
	if err != nil {
		return nil, status.Error(codes.Internal, "password hashing failed")
	}
	org, _ := s.store.Org()
	role := req.GetOrgRole()
	if role == zatterav1.Role_ROLE_UNSPECIFIED {
		role = zatterav1.Role_ROLE_DEVELOPER
	}
	id, _ := IdentityFrom(ctx)
	user := &zatterav1.User{
		Meta:         newMeta(ids.New(), s.clock.Now()),
		Email:        req.GetEmail(),
		DisplayName:  req.GetDisplayName(),
		PasswordHash: hash,
		OrgId:        org.GetMeta().GetId(),
		OrgRole:      role,
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutUser{PutUser: &clusterv1.PutUser{User: user}}}); err != nil {
		return nil, toStatus(err)
	}
	return redactUser(user), nil
}

// ListUsers lists all org users (admin only).
func (s *AuthServer) ListUsers(_ context.Context, _ *emptypb.Empty) (*zatterav1.ListUsersResponse, error) {
	users := s.store.ListUsers()
	for i := range users {
		users[i] = redactUser(users[i])
	}
	return &zatterav1.ListUsersResponse{Users: users}, nil
}

// apply stamps a command (caller sets cmd.Mutation) and proposes it.
func (s *AuthServer) apply(ctx context.Context, actorUser string, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "user:" + actorUser
	cmd.Time = timestamppb.Now()
	return s.raft.Apply(ctx, cmd)
}

func newMeta(id string, now time.Time) *zatterav1.Meta {
	ts := timestamppb.New(now)
	return &zatterav1.Meta{Id: id, CreatedAt: ts, UpdatedAt: ts}
}

func redactUser(u *zatterav1.User) *zatterav1.User {
	c := clone(u)
	c.PasswordHash = ""
	return c
}

func redactToken(t *zatterav1.Token) *zatterav1.Token {
	c := clone(t)
	c.SecretHash = ""
	return c
}

// --- argon2id password hashing (PHC string; params from secrets) ---

func hashPassword(password string) (string, error) {
	salt := make([]byte, secrets.ArgonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(password), salt, secrets.ArgonTime, secrets.ArgonMemoryKiB, secrets.ArgonThreads, secrets.ArgonKeyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, secrets.ArgonMemoryKiB, secrets.ArgonTime, secrets.ArgonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

func verifyPassword(phc, password string) bool {
	parts := strings.Split(phc, "$")
	// ["", "argon2id", "v=19", "m=..,t=..,p=..", salt, hash]
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false
	}
	var m, t uint32
	var p uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, t, m, p, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}
