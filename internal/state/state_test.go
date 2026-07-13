package state

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func meta(id string) *zatterav1.Meta {
	return &zatterav1.Meta{Id: id, CreatedAt: timestamppb.New(timestamppb.Now().AsTime())}
}

func TestUserCRUDAndEmailIndex(t *testing.T) {
	s := New()
	u := &zatterav1.User{Meta: meta("u1"), Email: "a@example.com", DisplayName: "A"}
	s.PutUser(u)

	got, ok := s.User("u1")
	if !ok || got.GetEmail() != "a@example.com" {
		t.Fatalf("User(u1) = %v, %v", got, ok)
	}
	if _, ok := s.UserByEmail("a@example.com"); !ok {
		t.Fatal("email index missing")
	}

	// Changing the email must move the index.
	u2 := &zatterav1.User{Meta: meta("u1"), Email: "b@example.com"}
	s.PutUser(u2)
	if _, ok := s.UserByEmail("a@example.com"); ok {
		t.Fatal("stale email index entry")
	}
	if _, ok := s.UserByEmail("b@example.com"); !ok {
		t.Fatal("new email index entry missing")
	}

	s.DeleteUser("u1")
	if _, ok := s.User("u1"); ok {
		t.Fatal("user not deleted")
	}
	if _, ok := s.UserByEmail("b@example.com"); ok {
		t.Fatal("email index not cleaned on delete")
	}
}

func TestReadsReturnClones(t *testing.T) {
	s := New()
	s.PutProject(&zatterav1.Project{Meta: meta("p1"), Name: "demo"})

	got, _ := s.Project("p1")
	got.Name = "mutated"

	again, _ := s.Project("p1")
	if again.GetName() != "demo" {
		t.Fatalf("store leaked internal pointer: name = %q", again.GetName())
	}

	// Mutating the input after Put must not affect the store either.
	in := &zatterav1.Project{Meta: meta("p2"), Name: "other"}
	s.PutProject(in)
	in.Name = "mutated"
	back, _ := s.Project("p2")
	if back.GetName() != "other" {
		t.Fatalf("store aliased input pointer: name = %q", back.GetName())
	}
}

func TestAssignmentNodeIndex(t *testing.T) {
	s := New()
	a := &zatterav1.Assignment{Meta: meta("a1"), NodeId: "n1", EnvironmentId: "e1"}
	s.PutAssignment(a)

	if got := s.ListAssignmentsByNode("n1"); len(got) != 1 {
		t.Fatalf("ListAssignmentsByNode(n1) = %d entries", len(got))
	}

	// Moving the assignment to another node updates the index.
	moved := &zatterav1.Assignment{Meta: meta("a1"), NodeId: "n2", EnvironmentId: "e1"}
	s.PutAssignment(moved)
	if got := s.ListAssignmentsByNode("n1"); len(got) != 0 {
		t.Fatalf("stale index on n1: %d entries", len(got))
	}
	if got := s.ListAssignmentsByNode("n2"); len(got) != 1 {
		t.Fatalf("index on n2: %d entries", len(got))
	}

	s.DeleteAssignments([]string{"a1"})
	if got := s.ListAssignmentsByNode("n2"); len(got) != 0 {
		t.Fatalf("index not cleaned on delete: %d entries", len(got))
	}
}

func TestSetAssignmentObservedGuards(t *testing.T) {
	s := New()
	s.PutAssignment(&zatterav1.Assignment{Meta: meta("a1"), NodeId: "n1"})

	obs := map[string]*zatterav1.AssignmentObserved{
		"a1":      {State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
		"missing": {State: zatterav1.InstanceState_INSTANCE_STATE_FAILED},
	}
	// Report from the wrong node is ignored.
	s.SetAssignmentObserved("n2", obs)
	a, _ := s.Assignment("a1")
	if a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_UNSPECIFIED {
		t.Fatal("observed update accepted from wrong node")
	}

	s.SetAssignmentObserved("n1", obs)
	a, _ = s.Assignment("a1")
	if a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
		t.Fatal("observed update not applied")
	}
}

func TestKVCAS(t *testing.T) {
	s := New()

	// expectedVersion 0 = create-if-absent.
	v, err := s.PutKV("certmagic/locks/x", []byte("a"), 0, 0)
	if err != nil || v != 1 {
		t.Fatalf("create: v=%d err=%v", v, err)
	}
	// Second create must conflict.
	if _, err := s.PutKV("certmagic/locks/x", []byte("b"), 0, 0); err != ErrKVConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
	// CAS on the right version succeeds.
	v, err = s.PutKV("certmagic/locks/x", []byte("b"), 1, 0)
	if err != nil || v != 2 {
		t.Fatalf("cas: v=%d err=%v", v, err)
	}
	// Unconditional always succeeds.
	if _, err := s.PutKV("certmagic/locks/x", []byte("c"), -1, 0); err != nil {
		t.Fatalf("unconditional: %v", err)
	}
	// Delete with wrong version conflicts; right version succeeds.
	if err := s.DeleteKV("certmagic/locks/x", 1); err != ErrKVConflict {
		t.Fatalf("delete cas: %v", err)
	}
	if err := s.DeleteKV("certmagic/locks/x", 3); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, _, ok := s.KV("certmagic/locks/x"); ok {
		t.Fatal("key still present")
	}
}

func TestWatchCoalescing(t *testing.T) {
	s := New()
	sub := s.Watch(KindProject)
	defer sub.Close()

	s.PutProject(&zatterav1.Project{Meta: meta("p1")})
	s.PutProject(&zatterav1.Project{Meta: meta("p1")}) // same key: coalesced
	s.PutProject(&zatterav1.Project{Meta: meta("p2")})
	s.PutUser(&zatterav1.User{Meta: meta("u1"), Email: "x@y"}) // filtered out

	<-sub.Notify()
	changes := sub.Drain()
	if len(changes) != 2 {
		t.Fatalf("expected 2 coalesced changes, got %d: %v", len(changes), changes)
	}
	for _, c := range changes {
		if c.Kind != KindProject {
			t.Fatalf("kind filter leaked: %v", c)
		}
	}
	if got := sub.Drain(); len(got) != 0 {
		t.Fatalf("drain not empty after drain: %v", got)
	}
}

func TestMarkAppliedIdempotency(t *testing.T) {
	s := New()
	if !s.MarkApplied("req-1", 10) {
		t.Fatal("first apply rejected")
	}
	if s.MarkApplied("req-1", 11) {
		t.Fatal("duplicate apply accepted")
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := New()
	s.SetOrg(&zatterav1.Org{Meta: meta("o1"), Name: "org"})
	s.PutUser(&zatterav1.User{Meta: meta("u1"), Email: "a@b.c"})
	s.PutProject(&zatterav1.Project{Meta: meta("p1"), Name: "demo"})
	s.PutProjectMember(&zatterav1.ProjectMember{ProjectId: "p1", UserId: "u1", Role: zatterav1.Role_ROLE_OWNER})
	s.PutApp(&zatterav1.App{Meta: meta("a1"), ProjectId: "p1", Name: "api"})
	s.PutEnvironment(&zatterav1.Environment{Meta: meta("e1"), AppId: "a1", ProjectId: "p1", Name: "production"})
	s.SetEnvVars("e1", map[string]*zatterav1.EncryptedValue{
		"KEY": {Nonce: []byte{1}, Ciphertext: []byte{2}},
	}, nil)
	s.PutRelease(&zatterav1.Release{Meta: meta("r1"), EnvironmentId: "e1", Version: 1})
	s.PutNode(&zatterav1.Node{Meta: meta("n1"), Name: "node-1", MeshIp: "10.90.0.1"})
	s.PutAssignment(&zatterav1.Assignment{Meta: meta("as1"), NodeId: "n1", EnvironmentId: "e1"})
	s.PutToken(&zatterav1.Token{Meta: meta("t1"), UserId: "u1", SecretHash: "abc"})
	s.PutDomain(&zatterav1.Domain{Meta: meta("d1"), ProjectId: "p1", Hostname: "api.example.com"})
	if _, err := s.PutKV("certmagic/certs/x", []byte("pem"), -1, 12345); err != nil {
		t.Fatal(err)
	}
	s.SetNetworkAllocation("p1", "e1", "n1", "10.201.0.0/24")
	s.SetServiceVIP("e1", "10.97.0.1")
	s.AppendAudit([]*zatterav1.AuditEntry{{Meta: meta("au1"), Method: "/zattera.v1.ProjectService/CreateProject"}})
	s.MarkApplied("req-1", 5)

	snap := s.SnapshotProto(42)

	restored := New()
	restored.RestoreProto(snap)

	// Round-trip must be lossless: snapshot the restore and compare.
	snap2 := restored.SnapshotProto(42)
	if !proto.Equal(snap, snap2) {
		t.Fatalf("snapshot round-trip not lossless:\n  a: %v\n  b: %v", snap, snap2)
	}

	// Indexes must be rebuilt.
	if _, ok := restored.UserByEmail("a@b.c"); !ok {
		t.Fatal("email index not rebuilt")
	}
	if _, ok := restored.TokenByHash("abc"); !ok {
		t.Fatal("token hash index not rebuilt")
	}
	if _, ok := restored.DomainByHostname("api.example.com"); !ok {
		t.Fatal("hostname index not rebuilt")
	}
	if got := restored.ListAssignmentsByNode("n1"); len(got) != 1 {
		t.Fatal("assignment node index not rebuilt")
	}
	if restored.MarkApplied("req-1", 6) {
		t.Fatal("applied-request set not rebuilt")
	}
	if _, v, exp, ok := restored.KV("certmagic/certs/x"); !ok || v != 1 || exp != 12345 {
		t.Fatalf("kv not restored: v=%d exp=%d ok=%v", v, exp, ok)
	}
}

func TestSnapshotDeterministicIgnoringMapOrder(t *testing.T) {
	// Snapshots are taken from maps; the proto compare in the round-trip test
	// relies on proto.Equal being order-sensitive for repeated fields. This
	// test documents that we sort nothing at snapshot time: equality between
	// two snapshots of the SAME store instance is not guaranteed order-wise,
	// so consumers must never diff raw snapshot bytes. What IS guaranteed:
	// restore(snapshot(s)) preserves content. Compare via restored stores.
	s := New()
	for _, id := range []string{"b", "a", "c"} {
		s.PutProject(&zatterav1.Project{Meta: meta(id), Name: id})
	}
	r1 := New()
	r1.RestoreProto(s.SnapshotProto(1))
	if len(r1.ListProjects()) != 3 {
		t.Fatal("restore lost projects")
	}
}

func TestEventRingCap(t *testing.T) {
	s := New()
	batch := make([]*zatterav1.Event, 0, 100)
	for i := 0; i < 100; i++ {
		batch = append(batch, &zatterav1.Event{Meta: meta("ev"), Kind: "test"})
	}
	for i := 0; i < eventRingCap/100+2; i++ {
		s.AppendEvents(batch)
	}
	if got := len(s.ListEvents(0)); got > eventRingCap {
		t.Fatalf("event ring exceeded cap: %d", got)
	}
}
