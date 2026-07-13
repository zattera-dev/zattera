// Package cliconfig manages the CLI-side configuration
// (~/.config/zattera/config.toml): named contexts pointing at clusters, with
// tokens. File mode 0600 — it contains credentials.
package cliconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Context is one cluster the CLI can talk to.
type Context struct {
	// Server is the API endpoint, e.g. "https://paas.example.com:8443".
	Server string `toml:"server"`
	Token  string `toml:"token"`
	// CACertPEM pins the cluster CA (dev mode / self-signed).
	CACertPEM string `toml:"ca_cert_pem,omitempty"`
	// DefaultProject for commands that take --project.
	DefaultProject string `toml:"default_project,omitempty"`
}

// File is the whole CLI config.
type File struct {
	CurrentContext string             `toml:"current_context"`
	Contexts       map[string]Context `toml:"contexts"`
}

// Path returns the config file location, honoring $ZATTERA_CONFIG.
func Path() (string, error) {
	if p := os.Getenv("ZATTERA_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "zattera", "config.toml"), nil
}

// Load reads the config; a missing file yields an empty config.
func Load() (*File, error) {
	path, err := Path()
	if err != nil {
		return nil, err
	}
	f := &File{Contexts: map[string]Context{}}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return f, nil
	}
	if _, err := toml.DecodeFile(path, f); err != nil {
		return nil, fmt.Errorf("cliconfig: %s: %w", path, err)
	}
	if f.Contexts == nil {
		f.Contexts = map[string]Context{}
	}
	return f, nil
}

// Save writes the config with 0600 permissions.
func (f *File) Save() error {
	path, err := Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := toml.NewEncoder(file).Encode(f); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Current returns the active context.
func (f *File) Current() (Context, bool) {
	c, ok := f.Contexts[f.CurrentContext]
	return c, ok
}
