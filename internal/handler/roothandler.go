package handler

import (
	"bytes"
	"net/http"

	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// RootHandler 首页
func RootHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewRootLogic(r.Context(), svcCtx)
		html, err := l.Root()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			var buf bytes.Buffer
			buf.WriteString(html)
			_, _ = w.Write(buf.Bytes())
			httpx.Ok(w)
		}
	}
}
