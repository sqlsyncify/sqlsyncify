package handler

import (
	"log"
	"net/http"
	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func SqlsyncifyAllHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authorizeAdmin(w, r, svcCtx.Config) {
			return
		}
		req, ok := parseSiteRequest(w, r)
		if !ok {
			return
		}
		if !lockSite(w, req.Site) {
			return
		}
		defer svc.UnlockSite(req.Site)
		log.Println("site", req.Site, "import", req.Import)

		l := logic.NewAllLogic(r.Context(), svcCtx)
		resp, err := l.All(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		_, _ = w.Write([]byte(resp.Message))
		httpx.Ok(w)
	}
}
