package proxy

import (
	"bufio"
	"compress/gzip"
	"errors"
	"net"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// checkBasicAuth reports whether the request satisfies the route's basic-auth
// requirement (true when none is configured). The stored secret is a bcrypt
// hash.
func checkBasicAuth(r *http.Request, ba *zatterav1.BasicAuth) bool {
	if ba == nil || ba.GetUsername() == "" {
		return true
	}
	u, p, ok := r.BasicAuth()
	if !ok || u != ba.GetUsername() {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(ba.GetPasswordHash()), []byte(p)) == nil
}

// parseCIDRs parses an allowlist of CIDRs and bare IPs into networks.
func parseCIDRs(list []string) []*net.IPNet {
	var out []*net.IPNet
	for _, s := range list {
		if _, n, err := net.ParseCIDR(s); err == nil {
			out = append(out, n)
			continue
		}
		if ip := net.ParseIP(s); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
		}
	}
	return out
}

// allowedIP reports whether the client address is within the allowlist (empty
// allowlist = allow all).
func allowedIP(remoteAddr string, cidrs []*net.IPNet) bool {
	if len(cidrs) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, c := range cidrs {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}

// wantsGzip reports whether the response for r should be gzip-compressed.
func wantsGzip(r *http.Request, compress bool) bool {
	if !compress {
		return false
	}
	if isUpgrade(r) {
		return false // never touch a websocket/upgrade
	}
	return strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
}

func isUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Connection"), "upgrade") || r.Header.Get("Upgrade") != ""
}

// respWriter records the status code and, when gzip is enabled, compresses the
// body (unless the upstream already set a Content-Encoding). It preserves
// Hijacker/Flusher so websocket and streaming responses keep working.
type respWriter struct {
	http.ResponseWriter
	gzipOn      bool
	status      int
	wroteHeader bool
	passthrough bool
	gz          *gzip.Writer
}

func newRespWriter(w http.ResponseWriter, gzipOn bool) *respWriter {
	return &respWriter{ResponseWriter: w, gzipOn: gzipOn, status: http.StatusOK}
}

func (rw *respWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.wroteHeader = true
	rw.status = code
	if rw.gzipOn {
		if rw.Header().Get("Content-Encoding") != "" {
			rw.passthrough = true // upstream already encoded
		} else {
			rw.Header().Set("Content-Encoding", "gzip")
			rw.Header().Del("Content-Length")
			rw.Header().Add("Vary", "Accept-Encoding")
		}
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *respWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	if rw.gzipOn && !rw.passthrough {
		if rw.gz == nil {
			rw.gz = gzip.NewWriter(rw.ResponseWriter)
		}
		return rw.gz.Write(b)
	}
	return rw.ResponseWriter.Write(b)
}

// finish flushes any gzip buffer; call after the handler returns.
func (rw *respWriter) finish() {
	if rw.gz != nil {
		_ = rw.gz.Close()
	}
}

func (rw *respWriter) Flush() {
	if rw.gz != nil {
		_ = rw.gz.Flush()
	}
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

var errNotHijacker = errors.New("proxy: response writer does not support hijacking")

func (rw *respWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errNotHijacker
}
