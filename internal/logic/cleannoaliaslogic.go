package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"
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
	addrs, err := utils.ParseESAddresses(siteConf.EsCluster)
	if err != nil {
		return err
	}
	esCfg := elasticsearch.Config{
		Addresses:     addrs,
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    5,
	}
	if key := strings.TrimSpace(siteConf.EsApiKey); key != "" {
		esCfg.APIKey = key
	}
	esClient, err := elasticsearch.NewTypedClient(esCfg)
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
		// Only this site's generated indexes. Other tenants and in-progress names
		// that do not match {prefix}_YYYYMMDDHHMMSS are left alone.
		if !utils.IsGeneratedSiteIndex(index, siteConf.IndexName, siteConf.AliasName) {
			continue
		}
		if alias.Aliases == nil || len(alias.Aliases) == 0 {
			delIndex = append(delIndex, index)
		} else {
			def, _ := json.Marshal(alias.Aliases)
			l.Logger.Info(index, " has alias: ", string(def))
		}
	}
	if len(delIndex) == 0 {
		log.Println("no index will delete")
		return nil
	}
	l.Logger.Info("deleted no alias index:", strings.Join(delIndex, ", "))
	req1 := esapi.IndicesDeleteRequest{Index: delIndex}
	resp1, err := req1.Do(l.ctx, esClient)
	if err != nil {
		return err
	}
	defer resp1.Body.Close()
	if resp1.IsError() {
		log.Println(resp1.String())
		return fmt.Errorf("delete index failed: status %d", resp1.StatusCode)
	}
	log.Println(resp1.String())
	return nil
}
