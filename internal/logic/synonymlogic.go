package logic

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	"log"
	"os"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
)

type SynonymLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSynonymLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SynonymLogic {
	return &SynonymLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// Synonym 获取网站的同义词配置
func (l *SynonymLogic) Synonym(req *types.SynonymRequest) (*types.SynonymResponse, error) {
	// TODO 用于不同的站点配置不同的数据源
	ymlFile := fmt.Sprintf("./etc/sites/%s/synonym.txt", req.Site)

	content, err := os.ReadFile(ymlFile)
	if err != nil {
		return nil, err
	}

	log.Println("load synonym, site:", req.Site)

	resp := types.SynonymResponse{}
	resp.Synonym = content

	return &resp, nil

}
