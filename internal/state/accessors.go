package state

import (
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// --- org & cluster key ---

func (s *Store) SetOrg(o *zatterav1.Org) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.org = clone(o)
	s.touch(KindOrg, o.GetMeta().GetId())
}

func (s *Store) Org() (*zatterav1.Org, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.org == nil {
		return nil, false
	}
	return clone(s.org), true
}

func (s *Store) SetClusterKeyMaterial(m *zatterav1.ClusterKeyMaterial) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterKey = clone(m)
	s.touch(KindClusterKey, "")
}

func (s *Store) ClusterKeyMaterial() (*zatterav1.ClusterKeyMaterial, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.clusterKey == nil {
		return nil, false
	}
	return clone(s.clusterKey), true
}

// --- users ---

func (s *Store) PutUser(u *zatterav1.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := u.GetMeta().GetId()
	if prev, ok := s.users[id]; ok {
		delete(s.usersByEmail, prev.GetEmail())
	}
	s.users[id] = clone(u)
	s.usersByEmail[u.GetEmail()] = id
	s.touch(KindUser, id)
}

func (s *Store) DeleteUser(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.users[id]; ok {
		delete(s.usersByEmail, prev.GetEmail())
		delete(s.users, id)
		s.touch(KindUser, id)
	}
}

func (s *Store) User(id string) (*zatterav1.User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return nil, false
	}
	return clone(u), true
}

func (s *Store) UserByEmail(email string) (*zatterav1.User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.usersByEmail[email]
	if !ok {
		return nil, false
	}
	return clone(s.users[id]), true
}

func (s *Store) ListUsers() []*zatterav1.User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.users))
}

// --- projects & members ---

func (s *Store) PutProject(p *zatterav1.Project) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := p.GetMeta().GetId()
	s.projects[id] = clone(p)
	s.touch(KindProject, id)
}

func (s *Store) DeleteProject(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.projects[id]; ok {
		delete(s.projects, id)
		delete(s.projectMembers, id)
		s.touch(KindProject, id)
	}
}

func (s *Store) Project(id string) (*zatterav1.Project, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.projects[id]
	if !ok {
		return nil, false
	}
	return clone(p), true
}

// ProjectByName resolves a project by its unique name.
func (s *Store) ProjectByName(name string) (*zatterav1.Project, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.projects {
		if p.GetName() == name {
			return clone(p), true
		}
	}
	return nil, false
}

func (s *Store) ListProjects() []*zatterav1.Project {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.projects))
}

func (s *Store) PutProjectMember(m *zatterav1.ProjectMember) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byUser, ok := s.projectMembers[m.GetProjectId()]
	if !ok {
		byUser = map[string]*zatterav1.ProjectMember{}
		s.projectMembers[m.GetProjectId()] = byUser
	}
	byUser[m.GetUserId()] = clone(m)
	s.touch(KindProjectMember, m.GetProjectId()+"/"+m.GetUserId())
}

func (s *Store) DeleteProjectMember(projectID, userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if byUser, ok := s.projectMembers[projectID]; ok {
		if _, ok := byUser[userID]; ok {
			delete(byUser, userID)
			s.touch(KindProjectMember, projectID+"/"+userID)
		}
	}
}

func (s *Store) ProjectMember(projectID, userID string) (*zatterav1.ProjectMember, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if byUser, ok := s.projectMembers[projectID]; ok {
		if m, ok := byUser[userID]; ok {
			return clone(m), true
		}
	}
	return nil, false
}

func (s *Store) ListProjectMembers(projectID string) []*zatterav1.ProjectMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byUser := s.projectMembers[projectID]
	out := make([]*zatterav1.ProjectMember, 0, len(byUser))
	for _, m := range byUser {
		out = append(out, clone(m))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetUserId() < out[j].GetUserId() })
	return out
}

// ListMembershipsOfUser returns all project memberships of one user.
func (s *Store) ListMembershipsOfUser(userID string) []*zatterav1.ProjectMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.ProjectMember
	for _, byUser := range s.projectMembers {
		if m, ok := byUser[userID]; ok {
			out = append(out, clone(m))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetProjectId() < out[j].GetProjectId() })
	return out
}

// --- apps ---

func (s *Store) PutApp(a *zatterav1.App) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := a.GetMeta().GetId()
	s.apps[id] = clone(a)
	s.touch(KindApp, id)
}

func (s *Store) DeleteApp(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.apps[id]; ok {
		delete(s.apps, id)
		s.touch(KindApp, id)
	}
}

func (s *Store) App(id string) (*zatterav1.App, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.apps[id]
	if !ok {
		return nil, false
	}
	return clone(a), true
}

func (s *Store) AppByName(projectID, name string) (*zatterav1.App, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.apps {
		if a.GetProjectId() == projectID && a.GetName() == name {
			return clone(a), true
		}
	}
	return nil, false
}

func (s *Store) ListApps(projectID string) []*zatterav1.App {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.App
	for _, a := range s.apps {
		if projectID == "" || a.GetProjectId() == projectID {
			out = append(out, clone(a))
		}
	}
	return sortByID(out)
}

// --- environments & env vars ---

func (s *Store) PutEnvironment(e *zatterav1.Environment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := e.GetMeta().GetId()
	s.environments[id] = clone(e)
	s.touch(KindEnvironment, id)
}

func (s *Store) DeleteEnvironment(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.environments[id]; ok {
		delete(s.environments, id)
		delete(s.envVars, id)
		s.touch(KindEnvironment, id)
	}
}

func (s *Store) Environment(id string) (*zatterav1.Environment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.environments[id]
	if !ok {
		return nil, false
	}
	return clone(e), true
}

func (s *Store) EnvironmentByName(appID, name string) (*zatterav1.Environment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.environments {
		if e.GetAppId() == appID && e.GetName() == name {
			return clone(e), true
		}
	}
	return nil, false
}

// ListEnvironments filters by app (or project when appID == "" and
// projectID != ""); both empty lists everything.
func (s *Store) ListEnvironments(projectID, appID string) []*zatterav1.Environment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Environment
	for _, e := range s.environments {
		if appID != "" && e.GetAppId() != appID {
			continue
		}
		if projectID != "" && e.GetProjectId() != projectID {
			continue
		}
		out = append(out, clone(e))
	}
	return sortByID(out)
}

// SetEnvVars applies a set/unset batch to an environment's variables.
func (s *Store) SetEnvVars(envID string, set map[string]*zatterav1.EncryptedValue, unset []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vars, ok := s.envVars[envID]
	if !ok {
		vars = map[string]*zatterav1.EncryptedValue{}
		s.envVars[envID] = vars
	}
	for k, v := range set {
		vars[k] = clone(v)
	}
	for _, k := range unset {
		delete(vars, k)
	}
	s.touch(KindEnvVar, envID)
}

func (s *Store) EnvVars(envID string) map[string]*zatterav1.EncryptedValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*zatterav1.EncryptedValue, len(s.envVars[envID]))
	for k, v := range s.envVars[envID] {
		out[k] = clone(v)
	}
	return out
}

// --- releases / deployments / builds ---

func (s *Store) PutRelease(r *zatterav1.Release) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := r.GetMeta().GetId()
	s.releases[id] = clone(r)
	s.touch(KindRelease, id)
}

func (s *Store) Release(id string) (*zatterav1.Release, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.releases[id]
	if !ok {
		return nil, false
	}
	return clone(r), true
}

// ListReleases returns an environment's releases sorted by version descending.
func (s *Store) ListReleases(envID string) []*zatterav1.Release {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Release
	for _, r := range s.releases {
		if envID == "" || r.GetEnvironmentId() == envID {
			out = append(out, clone(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetVersion() > out[j].GetVersion() })
	return out
}

// NextReleaseVersion returns 1 + the highest release version of the env.
func (s *Store) NextReleaseVersion(envID string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var maxV uint64
	for _, r := range s.releases {
		if r.GetEnvironmentId() == envID && r.GetVersion() > maxV {
			maxV = r.GetVersion()
		}
	}
	return maxV + 1
}

func (s *Store) PutDeployment(d *zatterav1.Deployment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := d.GetMeta().GetId()
	s.deployments[id] = clone(d)
	s.touch(KindDeployment, id)
}

func (s *Store) Deployment(id string) (*zatterav1.Deployment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.deployments[id]
	if !ok {
		return nil, false
	}
	return clone(d), true
}

func (s *Store) ListDeployments(envID string) []*zatterav1.Deployment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Deployment
	for _, d := range s.deployments {
		if envID == "" || d.GetEnvironmentId() == envID {
			out = append(out, clone(d))
		}
	}
	return sortByID(out)
}

func (s *Store) PutBuild(b *zatterav1.Build) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := b.GetMeta().GetId()
	s.builds[id] = clone(b)
	s.touch(KindBuild, id)
}

func (s *Store) Build(id string) (*zatterav1.Build, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.builds[id]
	if !ok {
		return nil, false
	}
	return clone(b), true
}

func (s *Store) ListBuilds(appID string) []*zatterav1.Build {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Build
	for _, b := range s.builds {
		if appID == "" || b.GetAppId() == appID {
			out = append(out, clone(b))
		}
	}
	return sortByID(out)
}

// --- helpers ---

type withMeta interface {
	proto.Message
	GetMeta() *zatterav1.Meta
}

func cloneMapValues[T withMeta](m map[string]T) []T {
	out := make([]T, 0, len(m))
	for _, v := range m {
		out = append(out, clone(v))
	}
	return out
}

func sortByID[T withMeta](in []T) []T {
	sort.Slice(in, func(i, j int) bool {
		return strings.Compare(in[i].GetMeta().GetId(), in[j].GetMeta().GetId()) < 0
	})
	return in
}
