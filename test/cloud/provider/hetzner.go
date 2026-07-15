package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// hetznerAPI is the Hetzner Cloud API base. Injectable for httptest.
const hetznerAPI = "https://api.hetzner.cloud/v1"

// Hetzner is a raw-REST Hetzner Cloud driver (no SDK — the surface is a handful
// of endpoints, matching roadmap T-83's mandate). It implements Driver plus the
// test-only extras the harness needs (SSH keys, firewalls, private networks).
type Hetzner struct {
	token   string
	baseURL string
	http    *http.Client
}

// NewHetzner builds a driver from an API token. baseURL defaults to the real
// API; pass a non-empty override (e.g. an httptest server) in tests.
func NewHetzner(token, baseURL string) *Hetzner {
	if baseURL == "" {
		baseURL = hetznerAPI
	}
	return &Hetzner{
		token:   token,
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

var _ Driver = (*Hetzner)(nil)

// --- Driver ---------------------------------------------------------------

func (h *Hetzner) Create(ctx context.Context, spec MachineSpec) (Machine, error) {
	image := spec.Image
	if image == "" {
		image = "debian-12"
	}
	body := map[string]any{
		"name":        spec.Name,
		"server_type": spec.ServerType,
		"image":       image,
		"location":    spec.Region,
		"user_data":   spec.CloudInit,
		"labels":      spec.Labels,
		"public_net": map[string]any{
			"enable_ipv4": boolOrTrue(spec.EnableIPv4),
			"enable_ipv6": boolOrTrue(spec.EnableIPv6),
		},
		// start_after_create defaults true; leave it.
	}
	if len(spec.SSHKeyIDs) > 0 {
		body["ssh_keys"] = spec.SSHKeyIDs
	}
	if len(spec.NetworkIDs) > 0 {
		body["networks"] = spec.NetworkIDs
	}

	var resp struct {
		Server hetznerServer `json:"server"`
	}
	if err := h.do(ctx, http.MethodPost, "/servers", body, &resp); err != nil {
		return Machine{}, err
	}
	return resp.Server.toMachine(), nil
}

func (h *Hetzner) Destroy(ctx context.Context, providerID string) error {
	err := h.do(ctx, http.MethodDelete, "/servers/"+providerID, nil, nil)
	if isNotFound(err) {
		return nil // idempotent: already gone is success
	}
	return err
}

func (h *Hetzner) Get(ctx context.Context, providerID string) (Machine, error) {
	var resp struct {
		Server hetznerServer `json:"server"`
	}
	if err := h.do(ctx, http.MethodGet, "/servers/"+providerID, nil, &resp); err != nil {
		if isNotFound(err) {
			return Machine{}, ErrMachineNotFound
		}
		return Machine{}, err
	}
	return resp.Server.toMachine(), nil
}

func (h *Hetzner) List(ctx context.Context, labelSelector map[string]string) ([]Machine, error) {
	path := "/servers"
	if sel := encodeLabelSelector(labelSelector); sel != "" {
		path += "?label_selector=" + sel
	}
	var resp struct {
		Servers []hetznerServer `json:"servers"`
	}
	if err := h.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	out := make([]Machine, 0, len(resp.Servers))
	for _, s := range resp.Servers {
		out = append(out, s.toMachine())
	}
	return out, nil
}

func (h *Hetzner) PriceEURPerHour(ctx context.Context, region, serverType string) (float64, error) {
	var resp struct {
		ServerTypes []struct {
			Prices []hetznerPrice `json:"prices"`
		} `json:"server_types"`
	}
	if err := h.do(ctx, http.MethodGet, "/server_types?name="+serverType, nil, &resp); err != nil {
		return 0, err
	}
	if len(resp.ServerTypes) == 0 {
		return 0, nil // unknown; caller falls back to the create-time price
	}
	for _, p := range resp.ServerTypes[0].Prices {
		if p.Location == region {
			return parsePrice(p.PriceHourly.Gross), nil
		}
	}
	return 0, nil
}

// --- test-only extras (never part of Driver) ------------------------------

// EnsureSSHKey uploads a public key under name (idempotent by name) and returns
// its provider ID. Hetzner rejects a duplicate name/fingerprint with 409/422;
// on conflict we look the existing key up and reuse it.
func (h *Hetzner) EnsureSSHKey(ctx context.Context, name, publicKey string, labels map[string]string) (int64, error) {
	body := map[string]any{"name": name, "public_key": publicKey, "labels": labels}
	var resp struct {
		SSHKey struct {
			ID int64 `json:"id"`
		} `json:"ssh_key"`
	}
	err := h.do(ctx, http.MethodPost, "/ssh_keys", body, &resp)
	if err == nil {
		return resp.SSHKey.ID, nil
	}
	if !isConflict(err) {
		return 0, err
	}
	// Already present — find it by name.
	var list struct {
		SSHKeys []struct {
			ID int64 `json:"id"`
		} `json:"ssh_keys"`
	}
	if lerr := h.do(ctx, http.MethodGet, "/ssh_keys?name="+name, nil, &list); lerr != nil {
		return 0, lerr
	}
	if len(list.SSHKeys) == 0 {
		return 0, fmt.Errorf("provider: ssh key %q conflicted but not found on lookup", name)
	}
	return list.SSHKeys[0].ID, nil
}

// DeleteSSHKey removes an uploaded key (idempotent).
func (h *Hetzner) DeleteSSHKey(ctx context.Context, id int64) error {
	err := h.do(ctx, http.MethodDelete, "/ssh_keys/"+strconv.FormatInt(id, 10), nil, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

// FirewallRule is one inbound/outbound rule.
type FirewallRule struct {
	Direction string   // "in" | "out"
	Protocol  string   // "tcp" | "udp" | "icmp"
	Port      string   // "8443" or "80-443"; empty for icmp
	SourceIPs []string // CIDRs; default ["0.0.0.0/0","::/0"] for "in"
	DestIPs   []string // CIDRs; default ["0.0.0.0/0","::/0"] for "out"
}

// CreateFirewall creates a firewall with the given rules, optionally applied to
// server IDs at creation. Returns the firewall ID.
func (h *Hetzner) CreateFirewall(ctx context.Context, name string, rules []FirewallRule, labels map[string]string, applyToServerIDs []int64) (int64, error) {
	apiRules := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		m := map[string]any{"direction": r.Direction, "protocol": r.Protocol}
		if r.Port != "" {
			m["port"] = r.Port
		}
		if r.Direction == "in" {
			m["source_ips"] = defaultCIDRs(r.SourceIPs)
		} else {
			m["destination_ips"] = defaultCIDRs(r.DestIPs)
		}
		apiRules = append(apiRules, m)
	}
	body := map[string]any{"name": name, "rules": apiRules, "labels": labels}
	if len(applyToServerIDs) > 0 {
		applyTo := make([]map[string]any, 0, len(applyToServerIDs))
		for _, id := range applyToServerIDs {
			applyTo = append(applyTo, map[string]any{"type": "server", "server": map[string]any{"id": id}})
		}
		body["apply_to"] = applyTo
	}
	var resp struct {
		Firewall struct {
			ID int64 `json:"id"`
		} `json:"firewall"`
	}
	if err := h.do(ctx, http.MethodPost, "/firewalls", body, &resp); err != nil {
		return 0, err
	}
	return resp.Firewall.ID, nil
}

// DeleteFirewall removes a firewall (idempotent). A firewall still applied to
// servers cannot be deleted; detach first with RemoveFirewallFromServers.
func (h *Hetzner) DeleteFirewall(ctx context.Context, id int64) error {
	err := h.do(ctx, http.MethodDelete, "/firewalls/"+strconv.FormatInt(id, 10), nil, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

// ApplyFirewallToServers attaches a firewall to servers (idempotent).
func (h *Hetzner) ApplyFirewallToServers(ctx context.Context, firewallID int64, serverIDs []int64) error {
	return h.firewallResourceAction(ctx, firewallID, "apply_to_resources", serverIDs)
}

// RemoveFirewallFromServers detaches a firewall from servers (idempotent).
func (h *Hetzner) RemoveFirewallFromServers(ctx context.Context, firewallID int64, serverIDs []int64) error {
	return h.firewallResourceAction(ctx, firewallID, "remove_from_resources", serverIDs)
}

func (h *Hetzner) firewallResourceAction(ctx context.Context, firewallID int64, action string, serverIDs []int64) error {
	resources := make([]map[string]any, 0, len(serverIDs))
	for _, id := range serverIDs {
		resources = append(resources, map[string]any{"type": "server", "server": map[string]any{"id": id}})
	}
	path := fmt.Sprintf("/firewalls/%d/actions/%s", firewallID, action)
	return h.do(ctx, http.MethodPost, path, map[string]any{"apply_to": resources}, nil)
}

// ListFirewalls returns firewall IDs matching the label selector.
func (h *Hetzner) ListFirewalls(ctx context.Context, labelSelector map[string]string) ([]int64, error) {
	path := "/firewalls"
	if sel := encodeLabelSelector(labelSelector); sel != "" {
		path += "?label_selector=" + sel
	}
	var resp struct {
		Firewalls []struct {
			ID        int64 `json:"id"`
			AppliedTo []struct {
				Server struct {
					ID int64 `json:"id"`
				} `json:"server"`
			} `json:"applied_to"`
		} `json:"firewalls"`
	}
	if err := h.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(resp.Firewalls))
	for _, f := range resp.Firewalls {
		ids = append(ids, f.ID)
	}
	return ids, nil
}

// ListNetworks returns private-network IDs matching the label selector.
func (h *Hetzner) ListNetworks(ctx context.Context, labelSelector map[string]string) ([]int64, error) {
	path := "/networks"
	if sel := encodeLabelSelector(labelSelector); sel != "" {
		path += "?label_selector=" + sel
	}
	var resp struct {
		Networks []struct {
			ID int64 `json:"id"`
		} `json:"networks"`
	}
	if err := h.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(resp.Networks))
	for _, n := range resp.Networks {
		ids = append(ids, n.ID)
	}
	return ids, nil
}

// CreateNetwork creates a private network with one cloud subnet (for NAT-node
// egress). Returns the network ID.
func (h *Hetzner) CreateNetwork(ctx context.Context, name, ipRange, zone string, labels map[string]string) (int64, error) {
	body := map[string]any{
		"name":     name,
		"ip_range": ipRange,
		"labels":   labels,
		"subnets": []map[string]any{
			{"type": "cloud", "ip_range": ipRange, "network_zone": zone},
		},
	}
	var resp struct {
		Network struct {
			ID int64 `json:"id"`
		} `json:"network"`
	}
	if err := h.do(ctx, http.MethodPost, "/networks", body, &resp); err != nil {
		return 0, err
	}
	return resp.Network.ID, nil
}

// DeleteNetwork removes a private network (idempotent).
func (h *Hetzner) DeleteNetwork(ctx context.Context, id int64) error {
	err := h.do(ctx, http.MethodDelete, "/networks/"+strconv.FormatInt(id, 10), nil, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

// AttachServerToNetwork attaches an existing server to a private network
// (idempotent: an already-attached server returns a conflict we swallow).
func (h *Hetzner) AttachServerToNetwork(ctx context.Context, providerID string, networkID int64) error {
	path := "/servers/" + providerID + "/actions/attach_to_network"
	err := h.do(ctx, http.MethodPost, path, map[string]any{"network": networkID}, nil)
	if isConflict(err) {
		return nil
	}
	return err
}

// AddRouteToNetwork installs a route in a private network (e.g. default route
// 0.0.0.0/0 via the NAT-gateway node's private IP), so no-public-IP nodes can
// egress through the gateway.
func (h *Hetzner) AddRouteToNetwork(ctx context.Context, networkID int64, destination, gatewayPrivateIP string) error {
	path := fmt.Sprintf("/networks/%d/actions/add_route", networkID)
	return h.do(ctx, http.MethodPost, path, map[string]any{"destination": destination, "gateway": gatewayPrivateIP}, nil)
}

// --- wire types + helpers -------------------------------------------------

type hetznerServer struct {
	ID        int64             `json:"id"`
	Name      string            `json:"name"`
	Status    string            `json:"status"`
	Created   string            `json:"created"`
	Labels    map[string]string `json:"labels"`
	PublicNet struct {
		IPv4 struct {
			IP string `json:"ip"`
		} `json:"ipv4"`
		IPv6 struct {
			IP string `json:"ip"`
		} `json:"ipv6"`
	} `json:"public_net"`
	PrivateNet []struct {
		IP string `json:"ip"`
	} `json:"private_net"`
	ServerType struct {
		Name         string         `json:"name"`
		Architecture string         `json:"architecture"` // "x86" | "arm"
		Prices       []hetznerPrice `json:"prices"`
	} `json:"server_type"`
}

type hetznerPrice struct {
	Location    string `json:"location"`
	PriceHourly struct {
		Gross string `json:"gross"`
	} `json:"price_hourly"`
}

func (s hetznerServer) toMachine() Machine {
	m := Machine{
		ProviderID: strconv.FormatInt(s.ID, 10),
		Name:       s.Name,
		Status:     normalizeStatus(s.Status),
		PublicIPv4: s.PublicNet.IPv4.IP,
		PublicIPv6: s.PublicNet.IPv6.IP,
		Labels:     s.Labels,
		Arch:       normalizeArch(s.ServerType.Architecture),
	}
	if len(s.PrivateNet) > 0 {
		m.PrivateIPv4 = s.PrivateNet[0].IP
	}
	if t, err := time.Parse(time.RFC3339, s.Created); err == nil {
		m.CreatedAt = t
	}
	// Price from the (create/get) response for the server's own location.
	for _, p := range s.ServerType.Prices {
		if len(s.ServerType.Prices) == 1 || p.Location != "" {
			m.HourlyPriceEUR = parsePrice(p.PriceHourly.Gross)
			break
		}
	}
	return m
}

// normalizeStatus maps Hetzner statuses onto the provider-agnostic set.
func normalizeStatus(s string) string {
	switch s {
	case "initializing", "starting":
		return StatusCreating
	case "running":
		return StatusRunning
	case "deleting", "stopping", "off":
		return StatusDeleting
	default:
		return StatusUnknown
	}
}

// normalizeArch maps Hetzner's "x86"/"arm" to GOARCH values.
func normalizeArch(a string) string {
	switch a {
	case "x86":
		return "amd64"
	case "arm":
		return "arm64"
	default:
		return a
	}
}

func parsePrice(s string) float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return f
}

// encodeLabelSelector renders {k:v} as the comma-joined "k==v" form Hetzner
// expects. Deterministic key order keeps request bodies test-stable.
func encodeLabelSelector(sel map[string]string) string {
	if len(sel) == 0 {
		return ""
	}
	keys := make([]string, 0, len(sel))
	for k := range sel {
		keys = append(keys, k)
	}
	sortStrings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"=="+sel[k])
	}
	return strings.Join(parts, ",")
}

func defaultCIDRs(in []string) []string {
	if len(in) > 0 {
		return in
	}
	return []string{"0.0.0.0/0", "::/0"}
}

func boolOrTrue(p *bool) bool {
	if p == nil {
		return true
	}
	return *p
}

// do performs one API call. It NEVER logs the token. A non-2xx response becomes
// an apiError carrying the status code so isNotFound/isConflict can classify it.
func (h *Hetzner) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests {
		// Honor Retry-After once is the driver's job in production (T-83); here
		// we surface it so the caller/test can decide. Never sleep-loop.
		return &apiError{status: resp.StatusCode, code: "rate_limited", message: "hetzner rate limited (Retry-After: " + resp.Header.Get("Retry-After") + ")"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseAPIError(resp)
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// apiError carries a Hetzner error response. The Authorization header is never
// included, so error values are safe to log.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("hetzner api: %d %s: %s", e.status, e.code, e.message)
}

func parseAPIError(resp *http.Response) error {
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = json.Unmarshal(b, &env)
	return &apiError{status: resp.StatusCode, code: env.Error.Code, message: env.Error.Message}
}

func isNotFound(err error) bool {
	var e *apiError
	return asAPIError(err, &e) && (e.status == http.StatusNotFound || e.code == "not_found")
}

func isConflict(err error) bool {
	var e *apiError
	return asAPIError(err, &e) && (e.status == http.StatusConflict || e.status == http.StatusUnprocessableEntity ||
		e.code == "uniqueness_error" || e.code == "invalid_input")
}

func asAPIError(err error, target **apiError) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*apiError); ok {
		*target = e
		return true
	}
	return false
}

// sortStrings is a tiny insertion sort so this package needs no sort import in
// its hot path (keeps the promotable file lean); n is always small.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
