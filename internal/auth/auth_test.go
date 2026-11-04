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
