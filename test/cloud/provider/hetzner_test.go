package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeHetzner records requests and replays canned responses for the endpoints
// the driver uses — the same shape T-83 will formalize under testdata/.
type fakeHetzner struct {
	t        *testing.T
	requests []recordedReq
}

type recordedReq struct {
	method string
	path   string // path + raw query
	body   map[string]any
}

func (f *fakeHetzner) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			f.t.Errorf("missing/wrong auth header: %q", got)
		}
		var body map[string]any
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			if len(b) > 0 {
				_ = json.Unmarshal(b, &body)
			}
		}
		f.requests = append(f.requests, recordedReq{method: r.Method, path: r.URL.RequestURI(), body: body})

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/servers":
			writeJSON(w, 201, map[string]any{"server": canServer(42, "initializing", "x86")})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/servers/"):
			writeJSON(w, 200, map[string]any{"server": canServer(42, "running", "x86")})
		case r.Method == http.MethodGet && r.URL.Path == "/servers":
			writeJSON(w, 200, map[string]any{"servers": []any{canServer(42, "running", "x86")}})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/servers/"):
			if strings.HasSuffix(r.URL.Path, "/404") {
				writeJSON(w, 404, map[string]any{"error": map[string]any{"code": "not_found", "message": "gone"}})
				return
			}
			w.WriteHeader(204)
		case r.Method == http.MethodGet && r.URL.Path == "/server_types":
			writeJSON(w, 200, map[string]any{"server_types": []any{map[string]any{
				"prices": []any{map[string]any{"location": "nbg1", "price_hourly": map[string]any{"gross": "0.0080"}}},
			}}})
		default:
			f.t.Errorf("unexpected request %s %s", r.Method, r.URL.RequestURI())
			w.WriteHeader(500)
		}
	}))
}

func canServer(id int, status, arch string) map[string]any {
	return map[string]any{
		"id":      id,
		"name":    "zt-x",
		"status":  status,
		"created": "2026-07-15T10:00:00+00:00",
		"labels":  map[string]any{"zattera-harness": "1"},
		"public_net": map[string]any{
			"ipv4": map[string]any{"ip": "203.0.113.10"},
			"ipv6": map[string]any{"ip": "2001:db8::1"},
		},
		"private_net": []any{map[string]any{"ip": "10.0.0.2"}},
		"server_type": map[string]any{
			"name":         "cx22",
			"architecture": arch,
			"prices":       []any{map[string]any{"location": "nbg1", "price_hourly": map[string]any{"gross": "0.0080"}}},
		},
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func newFake(t *testing.T) (*Hetzner, *fakeHetzner) {
	f := &fakeHetzner{t: t}
	srv := f.server()
	t.Cleanup(srv.Close)
	return NewHetzner("test-token", srv.URL), f
}

func TestHetznerCreateMapsResponse(t *testing.T) {
	h, f := newFake(t)
	tru := true
	m, err := h.Create(context.Background(), MachineSpec{
		Name: "zt-a", Region: "nbg1", ServerType: "cx22",
		CloudInit: "#cloud-config\n", Labels: map[string]string{"zattera-harness": "1"},
		EnableIPv6: &tru, SSHKeyIDs: []int64{7}, NetworkIDs: []int64{9},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if m.ProviderID != "42" || m.Status != StatusCreating || m.PublicIPv4 != "203.0.113.10" {
		t.Fatalf("mapped machine wrong: %+v", m)
	}
	if m.Arch != "amd64" || m.PublicIPv6 != "2001:db8::1" || m.PrivateIPv4 != "10.0.0.2" {
		t.Fatalf("harness fields wrong: %+v", m)
	}
	if m.HourlyPriceEUR != 0.008 {
		t.Fatalf("price = %v, want 0.008", m.HourlyPriceEUR)
	}
	// user_data + ssh_keys + networks must pass through the request body.
	req := f.requests[0]
	if req.body["user_data"] != "#cloud-config\n" {
		t.Fatalf("user_data not passed through: %v", req.body["user_data"])
	}
	pub, _ := req.body["public_net"].(map[string]any)
	if pub["enable_ipv4"] != true {
		t.Fatalf("enable_ipv4 default should be true: %v", pub)
	}
}

func TestHetznerCreateNoPublicIPv4(t *testing.T) {
	h, f := newFake(t)
	no := false
	if _, err := h.Create(context.Background(), MachineSpec{Name: "nat", Region: "nbg1", ServerType: "cx22", EnableIPv4: &no}); err != nil {
		t.Fatal(err)
	}
	pub := f.requests[0].body["public_net"].(map[string]any)
	if pub["enable_ipv4"] != false {
		t.Fatalf("NAT node must request enable_ipv4=false, got %v", pub["enable_ipv4"])
	}
}

func TestHetznerDestroyIdempotent(t *testing.T) {
	h, _ := newFake(t)
	if err := h.Destroy(context.Background(), "42"); err != nil {
		t.Fatalf("destroy existing: %v", err)
	}
	// A 404 delete is success (idempotent).
	if err := h.Destroy(context.Background(), "404"); err != nil {
		t.Fatalf("destroy of absent machine should be nil, got %v", err)
	}
}

func TestHetznerGetNotFound(t *testing.T) {
	// A server that 404s on GET → ErrMachineNotFound.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 404, map[string]any{"error": map[string]any{"code": "not_found", "message": "gone"}})
	}))
	defer srv.Close()
	h := NewHetzner("test-token", srv.URL)
	if _, err := h.Get(context.Background(), "99"); err != ErrMachineNotFound {
		t.Fatalf("want ErrMachineNotFound, got %v", err)
	}
}

func TestHetznerListEncodesSelector(t *testing.T) {
	h, f := newFake(t)
	if _, err := h.List(context.Background(), map[string]string{"zattera-harness": "1", "zattera-run": "abc"}); err != nil {
		t.Fatal(err)
	}
	// Deterministic key order → stable, assertable query.
	if !strings.Contains(f.requests[0].path, "label_selector=zattera-harness%3D%3D1%2Czattera-run%3D%3Dabc") &&
		!strings.Contains(f.requests[0].path, "label_selector=zattera-harness==1,zattera-run==abc") {
		t.Fatalf("selector encoding = %q", f.requests[0].path)
	}
}

func TestHetznerPrice(t *testing.T) {
	h, _ := newFake(t)
	p, err := h.PriceEURPerHour(context.Background(), "nbg1", "cx22")
	if err != nil || p != 0.008 {
		t.Fatalf("price = %v err=%v, want 0.008", p, err)
	}
}

func TestReapOlderThan(t *testing.T) {
	// A fake driver over canned machines: two old, one fresh.
	now := time.Unix(1_000_000, 0)
	d := &sliceDriver{machines: []Machine{
		{ProviderID: "old1", CreatedAt: now.Add(-4 * time.Hour)},
		{ProviderID: "old2", Labels: map[string]string{"zattera-created": "996400"}}, // ~1h before via label
		{ProviderID: "fresh", CreatedAt: now.Add(-10 * time.Minute)},
	}}
	destroyed, err := ReapOlderThan(context.Background(), d, map[string]string{"zattera-harness": "1"}, "zattera-created", 3*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(destroyed) != 1 || destroyed[0] != "old1" {
		t.Fatalf("reaper should destroy only old1 (fresh kept, old2 within maxAge via label), got %v", destroyed)
	}
}

// sliceDriver is a minimal in-memory Driver for the reaper test.
type sliceDriver struct{ machines []Machine }

func (s *sliceDriver) Create(context.Context, MachineSpec) (Machine, error) { return Machine{}, nil }
func (s *sliceDriver) Get(context.Context, string) (Machine, error) {
	return Machine{}, ErrMachineNotFound
}
func (s *sliceDriver) List(context.Context, map[string]string) ([]Machine, error) {
	return s.machines, nil
}
func (s *sliceDriver) PriceEURPerHour(context.Context, string, string) (float64, error) {
	return 0, nil
}
func (s *sliceDriver) Destroy(_ context.Context, id string) error {
	for i, m := range s.machines {
		if m.ProviderID == id {
			s.machines = append(s.machines[:i], s.machines[i+1:]...)
			return nil
		}
	}
	return nil
}
