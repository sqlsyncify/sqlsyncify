package logic

import (
	"context"
	"encoding/json"
	"log"
	"sqlsyncify/internal/types"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type CleanNoAliasLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCleanNoAliasLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CleanNoAliasLogic {
	return &CleanNoAliasLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CleanNoAliasLogic) CleanNoAlias(req *types.Request) error {
	siteConf, err := svc.NewSiteConf(req.Site)
	if err != nil {
		l.Error(req.Site, " failed to load site conf: ", err)
		return err
	}
	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: strings.Split(siteConf.EsCluster, ","),
		APIKey:    siteConf.EsApiKey,
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},
		// Retry up to 5 attempts
		//
		MaxRetries: 5,
	})
	if err != nil {
		return err
	}
	// 列出全部索引的别名, 也会带出没有别名的索引
	cat := esClient.Indices.GetAlias()
	mapResp, err := cat.Do(l.ctx)
	if err != nil {
		return err
	}
	var delIndex []string
	for index, alias := range mapResp {
		if strings.HasPrefix(index, ".") {
			continue
		}
		// 没有别名时是空map: len(mapObj) == 0
		//l.Logger.Info(index, " ", alias.Aliases)
		if alias.Aliases == nil || len(alias.Aliases) == 0 {
			delIndex = append(delIndex, index)
		} else {
			def, _ := json.Marshal(alias.Aliases)

			l.Logger.Info(index, " has alias: ", string(def))
		}
	}
	if len(delIndex) > 0 {
		l.Logger.Info("deleted no alias index:", strings.Join(delIndex, ", "))
		req1 := esapi.IndicesDeleteRequest{Index: delIndex}
		resp1, err := req1.Do(l.ctx, esClient)
		if err != nil {
			return err
		} else if resp1.IsError() {
			log.Println(resp1.String())
		} else {
			log.Println(resp1.String())
			_ = resp1.Body.Close()
		}
	} else {
		log.Println("no index will delete")
	}
	return nil
}
