package registry

import "net/http"

// Authenticator verifies registry HTTP basic-auth credentials. The registry
// package stays decoupled from cluster state: the daemon supplies an
// implementation backed by node registry credentials (KV registry/creds/<id>,
// created at join in T-17) and user personal access tokens (zpat_… as the
// password). A nil Authenticator disables auth (dev / tests).
type Authenticator interface {
	// Authenticate reports whether the username/password pair is valid.
	Authenticate(username, password string) bool
}

// AuthFunc adapts a plain function to the Authenticator interface.
type AuthFunc func(username, password string) bool

// Authenticate implements Authenticator.
func (f AuthFunc) Authenticate(username, password string) bool { return f(username, password) }

// authorize enforces basic auth when an Authenticator is configured. It returns
// true when the request may proceed. On failure it writes a 401 with a
// Basic challenge and an OCI UNAUTHORIZED error, and returns false.
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request) bool {
	if h.auth == nil {
		return true
	}
	user, pass, ok := r.BasicAuth()
	if ok && h.auth.Authenticate(user, pass) {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="zattera registry"`)
	h.writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
	return false
}
