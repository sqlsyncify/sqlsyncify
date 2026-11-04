package logic

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/zeromicro/go-zero/core/logx"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"
	"sqlsyncify/internal/utils"
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
	if !utils.CheckSiteFormat(req.Site) || !utils.CheckLangFormat(req.Lang) {
		return nil, errors.New("invalid site or lang")
	}
	path, err := utils.ResolveUnder("etc/sites", req.Site, "synonym.txt")
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("synonym not found")
		}
		log.Println("synonym read:", err)
		return nil, errors.New("synonym read failed")
	}

	log.Println("load synonym, site:", req.Site)

	resp := types.SynonymResponse{}
	resp.Synonym = content

	return &resp, nil

}
