package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultsValidate(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if !cfg.HasRole(RoleControl) || !cfg.HasRole(RoleWorker) {
		t.Fatal("default roles must be control+worker")
	}
}

func TestLoadTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
node_name = "node-a"
data_dir = "/tmp/zattera-test"
roles = ["worker"]
domain = "apps.example.com"

[join]
addr = "control.example.com:8443"
token = "K10abc::secret"

[mesh]
listen_port = 51999
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.NodeName != "node-a" || cfg.Mesh.ListenPort != 51999 || cfg.Join.Token != "K10abc::secret" {
		t.Fatalf("fields not loaded: %+v", cfg)
	}
	// Defaults survive partial files.
	if cfg.API.Listen != ":8443" {
		t.Fatalf("default lost: %q", cfg.API.Listen)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("node_nmae = \"typo\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown key accepted silently")
	}
}

func TestValidateRules(t *testing.T) {
	cfg := Default()
	cfg.Roles = []string{"worker"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("worker without join.addr must be rejected")
	}
	cfg.Join.Addr = "c:8443"
	if err := cfg.Validate(); err == nil {
		t.Fatal("join.addr without token must be rejected")
	}
	cfg.Join.Token = "K10x::y"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Roles = []string{"bogus"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("bogus role accepted")
	}
}
