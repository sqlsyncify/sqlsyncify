package auth

import (
	"crypto/subtle"
	"log"
	"net/http"
	"strings"
	"sync"
)

var missingTokenLog sync.Once

// Allow reports whether r carries the configured admin token.
// An empty token fails closed so a default deploy does not expose sync or clean.
func Allow(r *http.Request, token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		missingTokenLog.Do(func() {
			log.Println("admin API rejected: set SQLSYNCIFY_AUTH_TOKEN or AuthToken")
		})
		return false
	}
	got := tokenFromRequest(r)
	if len(got) != len(token) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

func tokenFromRequest(r *http.Request) string {
	if h := strings.TrimSpace(r.Header.Get("Authorization")); h != "" {
		const prefix = "bearer "
		if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
			return strings.TrimSpace(h[len(prefix):])
		}
		return ""
	}
	return strings.TrimSpace(r.Header.Get("X-Auth-Token"))
}
