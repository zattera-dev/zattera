// Package provider is the cloud-infrastructure abstraction the real-cluster
// test harness (test/cloud) uses to spin, inspect, and destroy VMs on a real
// provider (Hetzner today; others later).
//
// The Driver interface below is deliberately shaped like the FROZEN production
// autoscaling driver planned in Phase 8 (roadmap T-82,
// `internal/daemon/provision`): the same Create/Destroy/Get/List/
// PriceEURPerHour surface, the same normalized statuses, the same idempotent
// Destroy. The Hetzner implementation here is a working raw-REST prototype of
// T-83 — when Phase 8 lands, promote it into `internal/daemon/provision` and
// have this harness import that instead of carrying its own copy.
//
// Test-only concerns the production driver must NEVER grow — SSH keys, firewall
// and private-network manipulation, NAT simulation — live as EXTRA methods on
// the concrete *Hetzner type, not on the Driver interface. That keeps the
// interface promotable while giving the harness the superset it needs.
package provider

import (
	"context"
	"errors"
	"time"
)

// ErrMachineNotFound is returned by Get for an absent machine and lets Destroy
// treat "already gone" as success. Matches the planned provision.ErrMachineNotFound.
var ErrMachineNotFound = errors.New("provider: machine not found")

// Machine statuses, normalized across providers by the driver (never by
// callers). Mirrors the Phase 8 contract.
const (
	StatusCreating = "creating"
	StatusRunning  = "running"
	StatusDeleting = "deleting"
	StatusUnknown  = "unknown"
)

// MachineSpec describes a VM to create.
//
// The first five fields are the FROZEN T-82 core (do not rename — they are the
// promotion contract). The remaining fields are harness extensions the
// production driver may ignore or drop: T-83 hardcodes image=debian-12 and
// IPv4-only, but the harness needs mixed OS/arch and no-public-IP nodes to
// simulate NAT.
type MachineSpec struct {
	// --- frozen T-82 core ---
	Name       string            // RFC-1123: lowercase, digits, '-', ≤63
	Region     string            // provider location, e.g. "nbg1"
	ServerType string            // provider size, e.g. "cx22" (amd64) / "cax11" (arm64)
	CloudInit  string            // user-data (cloud-init); ≤32KiB on Hetzner
	Labels     map[string]string // provider-side labels for List()

	// --- harness extensions (production driver may drop) ---
	Image      string // OS image, e.g. "debian-12", "ubuntu-24.04"; default "debian-12"
	SSHKeyIDs  []int64
	EnableIPv4 *bool   // nil = true; false = no public IPv4 (NAT-node simulation)
	EnableIPv6 *bool   // nil = true
	NetworkIDs []int64 // private networks to attach at create (NAT egress)
}

// Machine is the provider-agnostic view of a created VM. Core fields match the
// planned production Machine; PublicIPv6/PrivateIPv4/CreatedAt are harness extras.
type Machine struct {
	// --- frozen T-82 core ---
	ProviderID     string
	Name           string
	Status         string // one of the Status* constants
	PublicIPv4     string
	HourlyPriceEUR float64
	Labels         map[string]string

	// --- harness extensions ---
	Arch        string    // normalized GOARCH: "amd64" | "arm64"
	PublicIPv6  string    // Hetzner hands out a /64; this is its base address
	PrivateIPv4 string    // first private-network IP, when attached
	CreatedAt   time.Time // provider-reported creation time
}

// Driver is the minimal, provider-agnostic machine lifecycle. It is the exact
// shape Phase 8's autoscaler will consume; keep it that way.
type Driver interface {
	Create(ctx context.Context, spec MachineSpec) (Machine, error)
	// Destroy is idempotent: an absent machine is success (nil error).
	Destroy(ctx context.Context, providerID string) error
	// Get returns ErrMachineNotFound when the machine is gone.
	Get(ctx context.Context, providerID string) (Machine, error)
	// List returns machines matching every label in the selector (AND).
	List(ctx context.Context, labelSelector map[string]string) ([]Machine, error)
	// PriceEURPerHour is the budget rail; 0 with nil error = unknown.
	PriceEURPerHour(ctx context.Context, region, serverType string) (float64, error)
}

// ReapOlderThan destroys every machine matching selector whose CreatedAt (or,
// missing that, the createdLabel value parsed as a unix timestamp) is older
// than maxAge. It is the cost safety net for keep-on-failure runs: a forgotten
// cluster self-destructs on the next harness start or CI cleanup. Returns the
// provider IDs it destroyed. Best-effort: a single Destroy failure is collected
// and reported but does not stop the sweep.
func ReapOlderThan(ctx context.Context, d Driver, selector map[string]string, createdLabel string, maxAge time.Duration, now time.Time) (destroyed []string, err error) {
	machines, listErr := d.List(ctx, selector)
	if listErr != nil {
		return nil, listErr
	}
	var errs []error
	for _, m := range machines {
		created := m.CreatedAt
		if created.IsZero() {
			if ts := parseUnixLabel(m.Labels[createdLabel]); !ts.IsZero() {
				created = ts
			}
		}
		if created.IsZero() || now.Sub(created) < maxAge {
			continue
		}
		if derr := d.Destroy(ctx, m.ProviderID); derr != nil {
			errs = append(errs, derr)
			continue
		}
		destroyed = append(destroyed, m.ProviderID)
	}
	return destroyed, errors.Join(errs...)
}

func parseUnixLabel(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	var sec int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return time.Time{}
		}
		sec = sec*10 + int64(c-'0')
	}
	return time.Unix(sec, 0)
}
