package logic

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"

	"github.com/zeromicro/go-zero/core/logx"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"
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
	if !utils.CheckSiteFormat(req.Site) || !utils.CheckLangFormat(req.Lang) {
		return nil, errors.New("invalid site or lang")
	}
	path, err := utils.ResolveUnder("etc/sites", req.Site, "synonym.txt")
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("synonym not found")
		}
		log.Println("synonym stat:", err)
		return nil, errors.New("synonym read failed")
	}

	log.Println("load synonym head, site:", req.Site)

	resp := types.SynonymHeadResponse{}
	resp.LastModified = stat.ModTime().String()
	resp.ETag = strconv.FormatInt(stat.ModTime().UnixMilli(), 10)

	return &resp, nil
}
