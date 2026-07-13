// Package ids generates the ULID identifiers used for every first-class
// object. ULIDs sort lexicographically by creation time, which is why list
// methods sorted by id are implicitly sorted by age.
package ids

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	mu      sync.Mutex
	entropy = ulid.Monotonic(rand.Reader, 0)
)

// New returns a fresh ULID string (26 chars, Crockford base32, lowercase-safe
// but canonically uppercase).
func New() string {
	mu.Lock()
	defer mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}

// Valid reports whether s parses as a ULID.
func Valid(s string) bool {
	_, err := ulid.ParseStrict(s)
	return err == nil
}
