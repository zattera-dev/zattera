package daemon

import (
	"bytes"
	"strings"
	"testing"

	"github.com/zattera-dev/zattera/internal/config"
)

func TestApplyDevDefaults(t *testing.T) {
	cfg := config.Default()
	cfg.Dev = true
	applyDevDefaults(&cfg)

	if cfg.Domain != devDomain {
		t.Errorf("domain = %q, want %q", cfg.Domain, devDomain)
	}
	if cfg.Ingress.HTTPListen != devIngressHTTP {
		t.Errorf("http = %q, want %q", cfg.Ingress.HTTPListen, devIngressHTTP)
	}
	if cfg.Ingress.HTTPSListen != devIngressHTTPS {
		t.Errorf("https = %q, want %q", cfg.Ingress.HTTPSListen, devIngressHTTPS)
	}

	// Explicit config is not overridden.
	custom := config.Default()
	custom.Dev = true
	custom.Domain = "apps.example.com"
	custom.Ingress.HTTPListen = ":8888"
	applyDevDefaults(&custom)
	if custom.Domain != "apps.example.com" || custom.Ingress.HTTPListen != ":8888" {
		t.Errorf("explicit config overridden: %+v", custom.Ingress)
	}
	// The still-default HTTPS port is switched to the dev port.
	if custom.Ingress.HTTPSListen != devIngressHTTPS {
		t.Errorf("https should default to dev port, got %q", custom.Ingress.HTTPSListen)
	}

	// Non-dev config is untouched.
	prod := config.Default()
	applyDevDefaults(&prod)
	if prod.Domain != "" || prod.Ingress.HTTPListen != prodIngressHTTP {
		t.Errorf("non-dev config changed: %+v", prod)
	}
}

func TestBootstrapSecrets(t *testing.T) {
	out := "Bootstrap admin token: zpat_abc123\nRecovery passphrase (STORE THIS SAFELY): apple-berry-cloud\n"
	tok, pass := bootstrapSecrets(out)
	if tok != "zpat_abc123" {
		t.Errorf("token = %q", tok)
	}
	if pass != "apple-berry-cloud" {
		t.Errorf("passphrase = %q", pass)
	}
	// A restart prints nothing → empty secrets.
	if tok, pass := bootstrapSecrets(""); tok != "" || pass != "" {
		t.Errorf("expected empty secrets, got %q/%q", tok, pass)
	}
}

// TestDevBannerSnapshot pins the banner format — T-54's smoke test parses the
// DEVBANNER: lines, so this guards their stability.
func TestDevBannerSnapshot(t *testing.T) {
	info := devBannerInfo{
		APIEndpoint:        "https://127.0.0.1:8443",
		Domain:             "apps.127.0.0.1.sslip.io",
		IngressHTTP:        "http://127.0.0.1:8080",
		IngressHTTPS:       "https://127.0.0.1:9443",
		RegistryEndpoint:   "https://127.0.0.1:5000",
		CACertPath:         "/var/lib/zattera/ca/ca.crt",
		DataDir:            "/var/lib/zattera",
		AdminToken:         "zpat_TESTTOKEN",
		RecoveryPassphrase: "apple-berry-cloud-delta",
	}
	var buf bytes.Buffer
	renderDevBanner(&buf, info)
	got := buf.String()

	// Machine-readable lines: exact set, exact values.
	wantLines := []string{
		"DEVBANNER:api=https://127.0.0.1:8443",
		"DEVBANNER:domain=apps.127.0.0.1.sslip.io",
		"DEVBANNER:ingress_http=http://127.0.0.1:8080",
		"DEVBANNER:ingress_https=https://127.0.0.1:9443",
		"DEVBANNER:registry=https://127.0.0.1:5000",
		"DEVBANNER:ca=/var/lib/zattera/ca/ca.crt",
		"DEVBANNER:data_dir=/var/lib/zattera",
		"DEVBANNER:token=zpat_TESTTOKEN",
	}
	for _, want := range wantLines {
		if !strings.Contains(got, "\n"+want+"\n") && !strings.HasPrefix(got, want+"\n") {
			t.Errorf("banner missing machine line: %q\n---\n%s", want, got)
		}
	}
	// Pretty block shows the token + a ready-to-run login command.
	if !strings.Contains(got, "Admin token (first boot") {
		t.Error("pretty block missing admin token")
	}
	if !strings.Contains(got, "zattera login --server https://127.0.0.1:8443 --ca-cert /var/lib/zattera/ca/ca.crt --token zpat_TESTTOKEN") {
		t.Errorf("banner missing login command:\n%s", got)
	}

	// Without first-boot secrets, no token lines appear.
	info.AdminToken, info.RecoveryPassphrase = "", ""
	var buf2 bytes.Buffer
	renderDevBanner(&buf2, info)
	if strings.Contains(buf2.String(), "DEVBANNER:token=") || strings.Contains(buf2.String(), "Admin token") {
		t.Errorf("restart banner should omit token:\n%s", buf2.String())
	}
}
