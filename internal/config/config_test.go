package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	if err := os.WriteFile(path, []byte("not_a_real_key = \"typo\"\n"), 0o600); err != nil {
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

// TestEveryKeyIsDocumented walks the config struct's toml tags and asserts each
// one appears in the configuration reference. Two keys (mesh.mode, upgrade.base_url)
// shipped undocumented; this keeps the page honest as the struct grows.
func TestEveryKeyIsDocumented(t *testing.T) {
	doc, err := os.ReadFile("../../docs/setup/configuration.md")
	if err != nil {
		t.Skipf("docs not available: %v", err)
	}
	page := string(doc)

	var walk func(reflect.Type)
	walk = func(rt reflect.Type) {
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag := f.Tag.Get("toml")
			if tag == "" {
				continue
			}
			if f.Type.Kind() == reflect.Struct {
				walk(f.Type)
				continue
			}
			if !strings.Contains(page, "`"+tag+"`") {
				t.Errorf("config key %q is not documented in docs/setup/configuration.md", tag)
			}
		}
	}
	walk(reflect.TypeOf(Config{}))
}
