package installer

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

var validScopes = map[string]bool{
	"read":    true,
	"write":   true,
	"content": true,
}

// GenerateToken generates a cryptographically random API bearer token (`pfapi_<43-char base64url>`),
// computes its lowercase SHA-256 hex digest, and renders a ready-to-paste YAML entry for
// postfix-api.conf. It never writes anything to disk.
func GenerateToken(name string, scopes []string) (rawToken string, sha256Hex string, yamlSnippet string, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", "", fmt.Errorf("token name must not be empty")
	}
	if strings.ContainsAny(name, "\r\n") {
		return "", "", "", fmt.Errorf("token name must not contain newlines")
	}

	normalizedScopes := make([]string, 0, len(scopes))
	seen := make(map[string]bool, len(scopes))
	for _, s := range scopes {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if !validScopes[s] {
			return "", "", "", fmt.Errorf("invalid token scope %q: allowed scopes are read, write, content", s)
		}
		if !seen[s] {
			seen[s] = true
			normalizedScopes = append(normalizedScopes, s)
		}
	}
	if len(normalizedScopes) == 0 {
		return "", "", "", fmt.Errorf("at least one valid scope (read, write, content) is required")
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", "", fmt.Errorf("generate random token bytes: %w", err)
	}
	rawToken = "pfapi_" + base64.RawURLEncoding.EncodeToString(buf)

	sum := sha256.Sum256([]byte(rawToken))
	sha256Hex = hex.EncodeToString(sum[:])

	var b strings.Builder
	b.WriteString("- name: ")
	b.WriteString(strconv.Quote(name))
	b.WriteString("\n  sha256: ")
	b.WriteString(strconv.Quote(sha256Hex))
	b.WriteString("\n  scopes:\n")
	for _, sc := range normalizedScopes {
		b.WriteString("    - ")
		b.WriteString(sc)
		b.WriteByte('\n')
	}

	return rawToken, sha256Hex, b.String(), nil
}
