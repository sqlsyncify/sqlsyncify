package logic

import (
	"context"

	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type RootLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRootLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RootLogic {
	return &RootLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RootLogic) Root() (string, error) {
	return `<p>sqlsyncify</p><p>Admin routes require Authorization: Bearer or X-Auth-Token.</p>`, nil
}
