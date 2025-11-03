package handler

import (
	"errors"
	"log"
	"net/http"
	"sqlsyncify/internal/logic"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// SynonymHeadHandler 这个http请求需要返回两个头部，一个是 Last-Modified，一个是 ETag
// 只要有一个发生变化，es插件就会去获取新的同义词来更新同义词
func SynonymHeadHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("SynonymHeadHandler")
		var req types.SynonymHeadRequest
		if err := httpx.Parse(r, &req); err != nil {
			httpx.Error(w, err)
			return
		}

		v := utils.CheckSiteFormat(req.Site)
		if !v {
			httpx.Error(w, errors.New("invalid site"))
			return
		}

		//查同义词文件最后修改日期
		//es同义词插件定时来取, 对比插件中存储的Last-Modified，两值不同就去读取同义词接口
		//es index setting中设置动态同义词interval时间1个小时以上，就不会造成压力
		l := logic.NewSynonymHeadLogic(r.Context(), svcCtx)
		resp, err := l.SynonymHead(&req)

		if err != nil {
			httpx.Error(w, err)
		} else {
			w.Header().Add("Last-Modified", resp.LastModified)

			//值	类型
			//ETag="123456789"	强校验
			//ETag=W/"123456789"	弱校验（W大小写敏感）
			if len(resp.ETag) < 1 {
				resp.ETag = resp.LastModified
			}
			w.Header().Add("ETag", resp.ETag)
			httpx.Ok(w)
		}

	}
}
