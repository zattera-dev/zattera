package api

import (
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/appconfig"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// TestEnvInjection covers T-50: sealed values decrypt into the AssignmentSet
// frame, platform variables (PORT/ZATTERA_ENV/ZATTERA_APP) are injected, a user
// PORT overrides the default, and an env-var change flows into the config hash.
func TestEnvInjection(t *testing.T) {
	dataKey, _ := secrets.GenerateDataKey()
	vault := mustVault(mustKeyring(dataKey, 1))

	st := state.New()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app1"}, Name: "web"})
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: "env1"}, AppId: "app1", Name: "production",
	})
	spec := &zatterav1.ServiceSpec{Ports: []*zatterav1.PortSpec{{Name: "http", ContainerPort: 3000}}}
	st.PutRelease(&zatterav1.Release{
		Meta: &zatterav1.Meta{Id: "rel1"}, EnvironmentId: "env1", AppId: "app1",
		ImageRef: "registry.example/web@sha256:abc", Service: spec,
	})

	sealed, err := vault.Seal([]byte("s3cr3t"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	st.SetEnvVars("env1", map[string]*zatterav1.EncryptedValue{"TOKEN": sealed}, nil)

	s := NewSyncServer(st, nil, livestate.New(clock.NewFake()), clock.NewFake(), nil, vault)
	a := &zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "a1"}, ReleaseId: "rel1",
		AppId: "app1", EnvironmentId: "env1",
	}
	rt := s.buildRuntime(a)
	if rt == nil {
		t.Fatal("expected a runtime payload")
	}
	env := rt.GetEnv()

	// Sealed value decrypts into the frame.
	if env["TOKEN"] != "s3cr3t" {
		t.Fatalf("TOKEN not decrypted: %+v", env)
	}
	// Platform-injected identity + port.
	if env["ZATTERA_ENV"] != "production" {
		t.Fatalf("ZATTERA_ENV = %q", env["ZATTERA_ENV"])
	}
	if env["ZATTERA_APP"] != "web" {
		t.Fatalf("ZATTERA_APP = %q", env["ZATTERA_APP"])
	}
	if env["PORT"] != "3000" {
		t.Fatalf("PORT = %q, want 3000", env["PORT"])
	}

	// A user-set PORT overrides the injected default.
	userPort, _ := vault.Seal([]byte("9999"))
	st.SetEnvVars("env1", map[string]*zatterav1.EncryptedValue{"PORT": userPort}, nil)
	if got := s.buildRuntime(a).GetEnv()["PORT"]; got != "9999" {
		t.Fatalf("user PORT override = %q, want 9999", got)
	}
}

// TestEnvInjectionHashChanges verifies an env-var change alters the config hash
// so the next deploy freezes a distinct release (T-50 step 2).
func TestEnvInjectionHashChanges(t *testing.T) {
	dataKey, _ := secrets.GenerateDataKey()
	vault := mustVault(mustKeyring(dataKey, 1))
	st := state.New()
	spec := &zatterav1.ServiceSpec{Ports: []*zatterav1.PortSpec{{Name: "http", ContainerPort: 8080}}}

	s := &DeployServer{store: st}

	base := appconfig.ConfigHash(spec, s.envVarVersion("env1")) // no vars

	v1, _ := vault.Seal([]byte("one"))
	st.SetEnvVars("env1", map[string]*zatterav1.EncryptedValue{"K": v1}, nil)
	withVar := appconfig.ConfigHash(spec, s.envVarVersion("env1"))
	if withVar == base {
		t.Fatal("adding an env var must change the config hash")
	}

	v2, _ := vault.Seal([]byte("two"))
	st.SetEnvVars("env1", map[string]*zatterav1.EncryptedValue{"K": v2}, nil)
	changed := appconfig.ConfigHash(spec, s.envVarVersion("env1"))
	if changed == withVar {
		t.Fatal("changing an env var value must change the config hash")
	}
}
