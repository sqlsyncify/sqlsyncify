package handler

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"sqlsyncify/internal/utils"
	"time"

	"github.com/zeromicro/go-zero/core/collection"

	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

var (
	cacheSynonym *collection.Cache
	err1         error
)

func init() {
	cacheSynonym, err1 = collection.NewCache(time.Hour*24, collection.WithName("cacheSynonym"))
	if err1 != nil {
		log.Fatal(err1)
	}
}

// SynonymHandler 这个http请求需要返回两个头部，一个是 Last-Modified，一个是 ETag
// 只要有一个发生变化，es插件就会去获取新的同义词来更新相应的同义词
func SynonymHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.SynonymRequest
		if err := httpx.Parse(r, &req); err != nil {
			httpx.Error(w, err)
			return
		}
		v := utils.CheckSiteFormat(req.Site)
		if !v {
			httpx.Error(w, errors.New("invalid site"))
			return
		}

		cacheKey := req.Site + "." + req.Lang
		if cached, ok := cacheSynonym.Get(cacheKey); !ok {
			l := logic.NewSynonymLogic(r.Context(), svcCtx)
			resp, err := l.Synonym(&req)

			if err != nil {
				httpx.Error(w, err)
			} else {
				texts := bytes.ReplaceAll(resp.Synonym, []byte("\\n"), []byte("\n"))
				_, _ = w.Write(append(texts, []byte("\n")...))
				cacheSynonym.SetWithExpire(cacheKey, texts, 24*time.Hour)
				httpx.Ok(w)
				//httpx.OkJsonCtx(r.Context(), w, resp.Synonym)
			}
		} else {
			_, _ = w.Write(cached.([]byte))
			httpx.Ok(w)
		}

	}
}
