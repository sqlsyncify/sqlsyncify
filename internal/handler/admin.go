package handler

import (
	"net/http"

	"sqlsyncify/internal/auth"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func authorizeAdmin(w http.ResponseWriter, r *http.Request, c config.Config) bool {
	if auth.Allow(r, c.AdminToken()) {
		return true
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

func parseSiteRequest(w http.ResponseWriter, r *http.Request) (types.Request, bool) {
	var req types.Request
	if err := httpx.Parse(r, &req); err != nil {
		httpx.ErrorCtx(r.Context(), w, err)
		return req, false
	}
	if !utils.CheckSiteFormat(req.Site) {
		http.Error(w, "invalid site", http.StatusBadRequest)
		return req, false
	}
	return req, true
}

func lockSite(w http.ResponseWriter, site string) bool {
	if svc.TryLockSite(site) {
		return true
	}
	http.Error(w, site+" already running", http.StatusConflict)
	return false
}
