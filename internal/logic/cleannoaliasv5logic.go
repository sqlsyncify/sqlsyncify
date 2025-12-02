package logic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"sqlsyncify/internal/types"
	"strings"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"

	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type CleanNoAliasV5Logic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// copy from v8
type IndexAliases struct {
	Aliases map[string]any `json:"aliases"`
}
type MapResponse map[string]IndexAliases

func NewCleanNoAliasV5Logic(ctx context.Context, svcCtx *svc.ServiceContext) *CleanNoAliasV5Logic {
	return &CleanNoAliasV5Logic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CleanNoAliasV5Logic) CleanNoAliasV5(req *types.Request) error {
	siteConf, err := svc.NewSiteConf(req.Site)
	if err != nil {
		l.Error(req.Site, " failed to load site conf: ", err)
		return err
	}
	esClientV5, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(siteConf.EsCluster, ","),
	})
	if err != nil {
		return err
	}
	// 列出全部索引的别名, 也会带出没有别名的索引
	esreq := esapi.IndicesGetAliasRequest{}
	resp, err := esreq.Do(l.ctx, esClientV5)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return errors.New(resp.String())
	}
	body := new(bytes.Buffer)
	_, _ = body.ReadFrom(resp.Body)
	defer resp.Body.Close()
	//return errors.New(body.String())

	//map
	//{"local_test_20241114173209":{"aliases":{}},", ... }
	var bodyMap MapResponse
	err = json.Unmarshal(body.Bytes(), &bodyMap)
	if err != nil {
		return err
	}

	var delIndex []string
	for index, alias := range bodyMap {
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
		resp1, err := req1.Do(l.ctx, esClientV5)
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
