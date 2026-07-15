//go:build cloud

package cloud

import (
	"encoding/base64"
	"strings"
)

// base64Encode renders bytes for a heredoc upload, wrapped so lines stay a sane
// length for the remote shell.
func base64Encode(b []byte) string {
	enc := base64.StdEncoding.EncodeToString(b)
	const width = 76
	var sb strings.Builder
	for i := 0; i < len(enc); i += width {
		end := i + width
		if end > len(enc) {
			end = len(enc)
		}
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(enc[i:end])
	}
	return sb.String()
}

// shQuote single-quotes a string for safe use in a POSIX shell command.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
