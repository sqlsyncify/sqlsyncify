package handler

import (
	"errors"
	"log"
	"net/http"
	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"
	"sync"

	"github.com/zeromicro/go-zero/rest/httpx"
)

var (
	mapLock sync.Map
)

func SqlsyncifyAllHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
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
		log.Println("site", req.Site, "import", req.Import)

		//1个站同时只能运行1个全量更新, 单机内存锁
		_, ok := mapLock.Load(req.Site)
		if ok {
			w.WriteHeader(http.StatusConflict)
			httpx.Error(w, errors.New(req.Site+" already running"))
			return
		}
		mapLock.Store(req.Site, 1)

		l := logic.NewAllLogic(r.Context(), svcCtx)
		resp, err := l.All(&req)
		mapLock.Delete(req.Site)

		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		_, _ = w.Write([]byte(resp.Message))
		httpx.Ok(w)

	}
}
