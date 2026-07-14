package api

import (
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/livestate"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// TestSyncServerRuntimePayload verifies that the control side resolves an
// assignment's release into an image + frozen spec and decrypts the
// environment's env vars into the per-assignment runtime payload (T-15 step 3).
func TestSyncServerRuntimePayload(t *testing.T) {
	dataKey, _ := secrets.GenerateDataKey()
	sealer, err := secrets.NewSealer(dataKey, 1)
	if err != nil {
		t.Fatalf("new sealer: %v", err)
	}

	st := state.New()
	st.PutRelease(&zatterav1.Release{
		Meta:          &zatterav1.Meta{Id: "rel1"},
		EnvironmentId: "env1",
		ImageRef:      "registry.example/app@sha256:abc",
		Service: &zatterav1.ServiceSpec{
			Ports: []*zatterav1.PortSpec{{Name: "http", ContainerPort: 8080}},
		},
	})
	sealed, err := sealer.Seal([]byte("s3cr3t"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	st.SetEnvVars("env1", map[string]*zatterav1.EncryptedValue{"TOKEN": sealed}, nil)

	s := NewSyncServer(st, nil, livestate.New(clock.NewFake()), clock.NewFake(), nil, sealer)

	rt := s.buildRuntime(&zatterav1.Assignment{
		Meta:          &zatterav1.Meta{Id: "a1"},
		ReleaseId:     "rel1",
		EnvironmentId: "env1",
	})
	if rt == nil {
		t.Fatal("expected a runtime payload")
	}
	if rt.GetImageRef() != "registry.example/app@sha256:abc" {
		t.Fatalf("image ref = %q", rt.GetImageRef())
	}
	if got := rt.GetSpec().GetPorts(); len(got) != 1 || got[0].GetName() != "http" {
		t.Fatalf("spec ports not carried: %+v", got)
	}
	if rt.GetEnv()["TOKEN"] != "s3cr3t" {
		t.Fatalf("env not decrypted: %+v", rt.GetEnv())
	}
	// Platform-injected PORT comes from the first container port (T-50).
	if rt.GetEnv()["PORT"] != "8080" {
		t.Fatalf("PORT not injected: %+v", rt.GetEnv())
	}

	// Unknown release → nil payload (agent will report FAILED).
	if s.buildRuntime(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "a2"}, ReleaseId: "missing"}) != nil {
		t.Fatal("unknown release should yield nil runtime payload")
	}

	// Without a sealer, sealed user vars are omitted but image/spec resolve and
	// platform vars (PORT) are still injected.
	noSeal := NewSyncServer(st, nil, livestate.New(clock.NewFake()), clock.NewFake(), nil, nil)
	rt2 := noSeal.buildRuntime(&zatterav1.Assignment{Meta: &zatterav1.Meta{Id: "a1"}, ReleaseId: "rel1", EnvironmentId: "env1"})
	if rt2 == nil || rt2.GetImageRef() == "" {
		t.Fatal("image/spec should resolve without a sealer")
	}
	if _, leaked := rt2.GetEnv()["TOKEN"]; leaked {
		t.Fatalf("sealed var must not appear without a sealer, got %+v", rt2.GetEnv())
	}
	if rt2.GetEnv()["PORT"] != "8080" {
		t.Fatalf("PORT should still be injected without a sealer, got %+v", rt2.GetEnv())
	}
}
