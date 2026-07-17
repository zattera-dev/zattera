package daemon

import (
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"time"

	"github.com/zattera-dev/zattera/internal/config"
)

// Dev-mode defaults (T-52). Ingress binds unprivileged ports so `--dev` runs
// without root; the domain resolves to loopback via sslip.io so app URLs work
// with no local DNS setup.
const (
	devDomain        = "apps.127.0.0.1.sslip.io"
	devIngressHTTP   = ":8080"
	devIngressHTTPS  = ":9443"
	devBannerPrefix  = "DEVBANNER:"
	prodIngressHTTP  = ":80"
	prodIngressHTTPS = ":443"
	// devRegistryListen avoids :5000, which macOS ControlCenter (AirPlay) owns.
	devRegistryListen  = ":5001"
	prodRegistryListen = ":5000"
	// devDrainWindow shortens the post-promotion blue-drain window for fast local
	// iteration (production keeps the 10-minute default). A deployment reaches
	// SUCCEEDED soon after promotion, so the scheduler and scale-to-zero loop
	// (which defer to a running deployment) resume promptly.
	devDrainWindow = 20 * time.Second
)

// applyDevDefaults fills unset dev-mode conveniences: a loopback app domain and
// unprivileged ingress ports (only when the production defaults are still in
// place, so an explicit config is never overridden).
func applyDevDefaults(cfg *config.Config) {
	if !cfg.Dev {
		return
	}
	if cfg.Domain == "" {
		cfg.Domain = devDomain
	}
	if cfg.Ingress.HTTPListen == prodIngressHTTP {
		cfg.Ingress.HTTPListen = devIngressHTTP
	}
	if cfg.Ingress.HTTPSListen == prodIngressHTTPS {
		cfg.Ingress.HTTPSListen = devIngressHTTPS
	}
	if cfg.Registry.Listen == prodRegistryListen {
		cfg.Registry.Listen = devRegistryListen
	}
	// Dev builds push/pull over plain HTTP so the host Docker daemon and
	// buildkitd need no extra CA trust for the loopback registry.
	cfg.Registry.InsecureHTTP = true
}

// devBannerInfo is everything the startup banner prints — and everything the
// T-54 smoke test parses from the DEVBANNER: lines.
type devBannerInfo struct {
	APIEndpoint      string
	Domain           string
	IngressHTTP      string
	IngressHTTPS     string
	RegistryEndpoint string
	CACertPath       string
	CAFingerprint    string // sha256 of the CA cert; usable as `login --ca-pin`
	DataDir          string
	// AdminToken / RecoveryPassphrase are set only on first boot.
	AdminToken         string
	RecoveryPassphrase string
}

// newDevBannerInfo derives the banner fields from the effective config.
func newDevBannerInfo(cfg config.Config) devBannerInfo {
	return devBannerInfo{
		APIEndpoint:      "https://127.0.0.1" + cfg.API.Listen,
		Domain:           cfg.Domain,
		IngressHTTP:      "http://127.0.0.1" + cfg.Ingress.HTTPListen,
		IngressHTTPS:     "https://127.0.0.1" + cfg.Ingress.HTTPSListen,
		RegistryEndpoint: "https://127.0.0.1" + cfg.Registry.Listen,
		CACertPath:       filepath.Join(cfg.DataDir, "ca", "ca.crt"),
		DataDir:          cfg.DataDir,
	}
}

// bootstrapSecrets extracts the admin token and recovery passphrase from
// Bootstrap's one-time stdout (empty on a restart, where nothing is reprinted).
var (
	tokenRe      = regexp.MustCompile(`Bootstrap admin token: (\S+)`)
	passphraseRe = regexp.MustCompile(`Recovery passphrase \(STORE THIS SAFELY\): (\S+)`)
)

func bootstrapSecrets(out string) (token, passphrase string) {
	if m := tokenRe.FindStringSubmatch(out); m != nil {
		token = m[1]
	}
	if m := passphraseRe.FindStringSubmatch(out); m != nil {
		passphrase = m[1]
	}
	return token, passphrase
}

// renderDevBanner writes the friendly startup block followed by machine-readable
// DEVBANNER: lines. The format is STABLE — T-54's smoke test parses the
// DEVBANNER: lines, so append new keys rather than reordering or renaming.
func renderDevBanner(w io.Writer, info devBannerInfo) {
	fmt.Fprintln(w, "┌─────────────────────────────────────────────────────────────┐")
	fmt.Fprintln(w, "│  Zattera dev mode — single node                             │")
	fmt.Fprintln(w, "└─────────────────────────────────────────────────────────────┘")
	fmt.Fprintf(w, "  API:        %s\n", info.APIEndpoint)
	fmt.Fprintf(w, "  App domain: %s\n", info.Domain)
	fmt.Fprintf(w, "  Ingress:    %s  (http)\n", info.IngressHTTP)
	fmt.Fprintf(w, "              %s  (https)\n", info.IngressHTTPS)
	fmt.Fprintf(w, "  Registry:   %s\n", info.RegistryEndpoint)
	fmt.Fprintf(w, "  CA cert:    %s\n", info.CACertPath)
	if info.CAFingerprint != "" {
		fmt.Fprintf(w, "  CA pin:     %s\n", info.CAFingerprint)
	}
	fmt.Fprintf(w, "  Data dir:   %s\n", info.DataDir)
	if info.AdminToken != "" {
		fmt.Fprintln(w, "")
		fmt.Fprintf(w, "  Admin token (first boot, store it):  %s\n", info.AdminToken)
		fmt.Fprintf(w, "  Recovery passphrase (STORE SAFELY):  %s\n", info.RecoveryPassphrase)
		fmt.Fprintln(w, "")
		fmt.Fprintf(w, "  Log in:  zattera login --server %s --ca-cert %s --token %s\n",
			info.APIEndpoint, info.CACertPath, info.AdminToken)
	}

	// Machine-readable lines (stable keys; see T-54).
	fmt.Fprintf(w, "%sapi=%s\n", devBannerPrefix, info.APIEndpoint)
	fmt.Fprintf(w, "%sdomain=%s\n", devBannerPrefix, info.Domain)
	fmt.Fprintf(w, "%singress_http=%s\n", devBannerPrefix, info.IngressHTTP)
	fmt.Fprintf(w, "%singress_https=%s\n", devBannerPrefix, info.IngressHTTPS)
	fmt.Fprintf(w, "%sregistry=%s\n", devBannerPrefix, info.RegistryEndpoint)
	fmt.Fprintf(w, "%sca=%s\n", devBannerPrefix, info.CACertPath)
	if info.CAFingerprint != "" {
		fmt.Fprintf(w, "%sca_fingerprint=%s\n", devBannerPrefix, info.CAFingerprint)
	}
	fmt.Fprintf(w, "%sdata_dir=%s\n", devBannerPrefix, info.DataDir)
	if info.AdminToken != "" {
		fmt.Fprintf(w, "%stoken=%s\n", devBannerPrefix, info.AdminToken)
	}
}
