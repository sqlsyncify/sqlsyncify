package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAllowFailsClosed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/sync/all/wordpress", nil)
	if Allow(req, "") {
		t.Fatal("empty token must be rejected")
	}
	if Allow(req, "correct-token") {
		t.Fatal("missing header must be rejected")
	}
}

func TestAllowBearerAndHeader(t *testing.T) {
	const token = "correct-token"
	bearer := httptest.NewRequest(http.MethodGet, "/", nil)
	bearer.Header.Set("Authorization", "Bearer "+token)
	if !Allow(bearer, token) {
		t.Fatal("bearer token should be accepted")
	}

	header := httptest.NewRequest(http.MethodGet, "/", nil)
	header.Header.Set("X-Auth-Token", token)
	if !Allow(header, token) {
		t.Fatal("X-Auth-Token should be accepted")
	}

	wrong := httptest.NewRequest(http.MethodGet, "/", nil)
	wrong.Header.Set("Authorization", "Bearer nope")
	if Allow(wrong, token) {
		t.Fatal("wrong token should be rejected")
	}
}

func TestMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	protected := Middleware(func() string { return "secret" })(next)

	missing := httptest.NewRequest(http.MethodGet, "/sync/all/wordpress", nil)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, missing)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d", rec.Code)
	}

	ok := httptest.NewRequest(http.MethodGet, "/sync/all/wordpress", nil)
	ok.Header.Set("X-Auth-Token", "secret")
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, ok)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid token status = %d", rec.Code)
	}

	closed := Middleware(func() string { return "" })(next)
	rec = httptest.NewRecorder()
	closed.ServeHTTP(rec, ok)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("empty configured token status = %d", rec.Code)
	}
}
