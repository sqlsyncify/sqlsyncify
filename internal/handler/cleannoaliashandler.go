package handler

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"

	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// CleanNoAliasHandler 清除没有别名的索引, 腾出es缓存和空间
func CleanNoAliasHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.Request
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		v := utils.CheckSiteFormat(req.Site)
		if !v {
			httpx.Error(w, errors.New("invalid site"))
			return
		}
		log.Println("site", req.Site)

		l := logic.NewCleanNoAliasLogic(r.Context(), svcCtx)
		err := l.CleanNoAlias(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			var buf bytes.Buffer
			buf.WriteString("OK")
			_, _ = w.Write(buf.Bytes())
			httpx.Ok(w)
		}
	}
}
