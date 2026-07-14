package api

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/state"
)

// minRole is the minimum PROJECT role a caller needs for each project-scoped
// method. Methods absent here are not project-scoped and are governed only by
// the T-04 tier table. Org owners/admins bypass these checks entirely.
var minRole = map[string]zatterav1.Role{
	// ProjectService
	"/zattera.v1.ProjectService/GetProject":    zatterav1.Role_ROLE_VIEWER,
	"/zattera.v1.ProjectService/DeleteProject": zatterav1.Role_ROLE_OWNER,
	"/zattera.v1.ProjectService/ListMembers":   zatterav1.Role_ROLE_VIEWER,
	"/zattera.v1.ProjectService/AddMember":     zatterav1.Role_ROLE_ADMIN,
	"/zattera.v1.ProjectService/RemoveMember":  zatterav1.Role_ROLE_ADMIN,

	// AppService (T-06)
	"/zattera.v1.AppService/CreateApp":      zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.AppService/ListApps":       zatterav1.Role_ROLE_VIEWER,
	"/zattera.v1.AppService/GetApp":         zatterav1.Role_ROLE_VIEWER,
	"/zattera.v1.AppService/DeleteApp":      zatterav1.Role_ROLE_ADMIN,
	"/zattera.v1.AppService/ApplyAppConfig": zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.AppService/SetEnvVars":     zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.AppService/GetEnvVars":     zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.AppService/SetReplicas":    zatterav1.Role_ROLE_DEVELOPER,

	// DeployService (T-25/T-26)
	"/zattera.v1.DeployService/Deploy":   zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.DeployService/Rollback": zatterav1.Role_ROLE_DEVELOPER,

	// DomainService (T-45)
	"/zattera.v1.DomainService/AddDomain":     zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.DomainService/RemoveDomain":  zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.DomainService/SetMiddleware": zatterav1.Role_ROLE_DEVELOPER,
	"/zattera.v1.DomainService/ListDomains":   zatterav1.Role_ROLE_VIEWER,
}

// RBAC enforces per-project roles. It resolves the request's project_id field
// (accepting a project NAME and rewriting it to the canonical id) and compares
// the caller's effective role to minRole.
type RBAC struct {
	store *state.Store
}

// NewRBAC builds the RBAC interceptor.
func NewRBAC(store *state.Store) *RBAC { return &RBAC{store: store} }

// UnaryInterceptor authorizes project-scoped unary calls.
func (r *RBAC) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	required, scoped := minRole[info.FullMethod]
	if !scoped {
		return handler(ctx, req)
	}
	msg, ok := req.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "rbac: request is not a proto message")
	}
	if err := r.check(ctx, msg, required); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// check resolves the project and enforces the role. It rewrites the request's
// project_id field to the canonical id so handlers always see an id.
func (r *RBAC) check(ctx context.Context, msg proto.Message, required zatterav1.Role) error {
	id, ok := IdentityFrom(ctx)
	if !ok || id.UserID == "" {
		return status.Error(codes.Unauthenticated, "a user identity is required")
	}

	fd := msg.ProtoReflect().Descriptor().Fields().ByName("project_id")
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		// No project scope on this message; the tier table already governed it.
		return nil
	}
	ref := msg.ProtoReflect()
	raw := ref.Get(fd).String()
	if raw == "" {
		return status.Error(codes.InvalidArgument, "project is required")
	}
	proj, err := r.resolveProject(raw)
	if err != nil {
		return err
	}
	// Canonicalize the field to the resolved id.
	ref.Set(fd, protoreflect.ValueOfString(proj.GetMeta().GetId()))

	// Org owners/admins bypass project membership.
	if user, ok := r.store.User(id.UserID); ok {
		if user.GetOrgRole() == zatterav1.Role_ROLE_OWNER || user.GetOrgRole() == zatterav1.Role_ROLE_ADMIN {
			return nil
		}
	}
	member, ok := r.store.ProjectMember(proj.GetMeta().GetId(), id.UserID)
	if !ok {
		// Non-members must not learn the project exists.
		return status.Error(codes.NotFound, "project not found")
	}
	if roleRank(member.GetRole()) < roleRank(required) {
		return status.Errorf(codes.PermissionDenied, "%s role required", required)
	}
	return nil
}

// resolveProject accepts a project id or name.
func (r *RBAC) resolveProject(idOrName string) (*zatterav1.Project, error) {
	if p, ok := r.store.Project(idOrName); ok {
		return p, nil
	}
	if p, ok := r.store.ProjectByName(idOrName); ok {
		return p, nil
	}
	return nil, status.Errorf(codes.NotFound, "project %q not found", idOrName)
}

// roleRank maps the inverted role enum (OWNER=1 … VIEWER=4) to an ascending
// privilege rank so comparisons read naturally.
func roleRank(role zatterav1.Role) int {
	switch role {
	case zatterav1.Role_ROLE_OWNER:
		return 4
	case zatterav1.Role_ROLE_ADMIN:
		return 3
	case zatterav1.Role_ROLE_DEVELOPER:
		return 2
	case zatterav1.Role_ROLE_VIEWER:
		return 1
	default:
		return 0
	}
}

// isOrgAdmin reports whether the identity is an org owner/admin.
func (r *RBAC) isOrgAdmin(userID string) bool {
	u, ok := r.store.User(userID)
	if !ok {
		return false
	}
	return u.GetOrgRole() == zatterav1.Role_ROLE_OWNER || u.GetOrgRole() == zatterav1.Role_ROLE_ADMIN
}
