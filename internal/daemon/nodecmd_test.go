package daemon

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/zattera-dev/zattera/internal/config"
)

// The generated config must be valid TOML that config.Load accepts with no
// unknown keys and that passes Validate — otherwise `cluster init/join` writes a
// file the daemon refuses to start on.
func TestControlConfigRoundTrips(t *testing.T) {
	toml := controlConfigTOML("mario", "/var/lib/zattera", "apps.example.com",
		"mario.example.com", "203.0.113.10", "ops@example.com", true)
	cfg := loadTOML(t, toml)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("control config invalid: %v", err)
	}
	if cfg.NodeName != "mario" || cfg.Domain != "apps.example.com" {
		t.Fatalf("name/domain: %+v", cfg)
	}
	if !cfg.HasRole(config.RoleControl) || !cfg.HasRole(config.RoleWorker) {
		t.Fatalf("roles: %v", cfg.Roles)
	}
	if cfg.API.AdvertiseAddr != "mario.example.com:8443" {
		t.Fatalf("advertise: %q", cfg.API.AdvertiseAddr)
	}
	if len(cfg.Mesh.PublicEndpoints) != 1 || cfg.Mesh.PublicEndpoints[0] != "203.0.113.10:51820" {
		t.Fatalf("mesh endpoints: %v", cfg.Mesh.PublicEndpoints)
	}
	if cfg.ACME.Email != "ops@example.com" || !cfg.ACME.Staging {
		t.Fatalf("acme: %+v", cfg.ACME)
	}
}

func TestControlConfigOmitsACMEWhenNoEmail(t *testing.T) {
	cfg := loadTOML(t, controlConfigTOML("n", "/d", "apps.x.com", "1.2.3.4", "1.2.3.4", "", false))
	if cfg.ACME.Email != "" {
		t.Fatalf("expected no acme email, got %q", cfg.ACME.Email)
	}
}

func TestWorkerConfigRoundTrips(t *testing.T) {
	cfg := loadTOML(t, workerConfigTOML("luigi", "/var/lib/zattera", "203.0.113.10:8443", "zjoin_abc", "198.51.100.7"))
	if err := cfg.Validate(); err != nil {
		t.Fatalf("worker config invalid: %v", err)
	}
	if cfg.HasRole(config.RoleControl) {
		t.Fatal("worker must not have the control role")
	}
	if cfg.Join.Addr != "203.0.113.10:8443" || cfg.Join.Token != "zjoin_abc" {
		t.Fatalf("join: %+v", cfg.Join)
	}
	if len(cfg.Mesh.PublicEndpoints) != 1 || cfg.Mesh.PublicEndpoints[0] != "198.51.100.7:51820" {
		t.Fatalf("mesh endpoints: %v", cfg.Mesh.PublicEndpoints)
	}
}

func TestWorkerConfigNoMeshEndpointWhenIPUnknown(t *testing.T) {
	cfg := loadTOML(t, workerConfigTOML("luigi", "/d", "1.2.3.4:8443", "zjoin_abc", ""))
	if err := cfg.Validate(); err != nil {
		t.Fatalf("worker config invalid: %v", err)
	}
	if len(cfg.Mesh.PublicEndpoints) != 0 {
		t.Fatalf("expected no mesh endpoints, got %v", cfg.Mesh.PublicEndpoints)
	}
}

// caFingerprintPEM must equal hex(sha256(cert.Raw)) — the value login --ca-pin
// checks and daemon.caFingerprint emits.
func TestCAFingerprintMatchesDaemon(t *testing.T) {
	der := selfSignedDER(t)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	got, err := caFingerprintPEM(pemBytes)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(der)
	if want := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("fingerprint = %s, want %s", got, want)
	}
}

func TestFirstLabel(t *testing.T) {
	for in, want := range map[string]string{
		"apps.example.com": "example",
		"apps.zattera.dev": "zattera",
		"localhost":        "localhost",
		"a.b.c.d":          "c",
	} {
		if got := firstLabel(in); got != want {
			t.Errorf("firstLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func loadTOML(t *testing.T, content string) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load rejected generated config: %v\n---\n%s", err, content)
	}
	return cfg
}

func selfSignedDER(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}
