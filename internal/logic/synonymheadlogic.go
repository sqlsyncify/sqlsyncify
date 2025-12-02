package logic

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	"log"
	"os"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
	"strconv"
)

type SynonymHeadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSynonymHeadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SynonymHeadLogic {
	return &SynonymHeadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// SynonymHead 获取网站的同义词配置
func (l *SynonymHeadLogic) SynonymHead(req *types.SynonymHeadRequest) (*types.SynonymHeadResponse, error) {
	// 查配置文件最后更改日期
	// TODO 用于不同的站点配置不同的数据源
	ymlFile := fmt.Sprintf("./etc/sites/%s/synonym.txt", req.Site)
	stat, err := os.Stat(ymlFile)
	if os.IsNotExist(err) {
		return nil, err
	}

	log.Println("load synonym head, site:", req.Site)

	resp := types.SynonymHeadResponse{}
	resp.LastModified = stat.ModTime().String()
	resp.ETag = strconv.FormatInt(stat.ModTime().UnixMilli(), 10)

	return &resp, nil
}
