package handler

import (
	"log"
	"net/http"
	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// CleanNoAliasHandler 清除本站点没有别名的生成索引
func CleanNoAliasHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
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
		log.Println("site", req.Site)

		l := logic.NewCleanNoAliasLogic(r.Context(), svcCtx)
		if err := l.CleanNoAlias(&req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		_, _ = w.Write([]byte("OK"))
		httpx.Ok(w)
	}
}
