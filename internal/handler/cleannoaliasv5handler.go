package handler

import (
	"log"
	"net/http"
	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// CleanNoAliasV5Handler 清除本站点没有别名的生成索引（Elasticsearch 5.x）
func CleanNoAliasV5Handler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := parseSiteRequest(w, r)
		if !ok {
			return
		}
		if !lockSite(w, req.Site) {
			return
		}
		defer svc.UnlockSite(req.Site)
		log.Println("site", req.Site)

		l := logic.NewCleanNoAliasV5Logic(r.Context(), svcCtx)
		if err := l.CleanNoAliasV5(&req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		_, _ = w.Write([]byte("OK"))
		httpx.Ok(w)
	}
}
