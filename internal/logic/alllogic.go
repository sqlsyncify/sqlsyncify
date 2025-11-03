package logic

import (
	"context"
	"fmt"
	"log"
	"sqlsyncify/internal/logic/export"
	"sqlsyncify/internal/logic/importer"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/types"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"
)

type AllLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAllLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AllLogic {
	return &AllLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *AllLogic) All(req *types.Request) (resp *types.Response, err error) {
	siteConf, err := svc.NewSiteConf(req.Site)
	if err != nil {
		l.Error(req.Site, " failed to load site conf: ", err)
		return
	}
	db, err := svc.NewDbConn(siteConf.DataSource)
	if err != nil {
		l.Error(req.Site, " failed to connect to DataSource: ", err)
		return
	}

	//prepare
	dbLocal, err := svc.NewSqliteConn(req.Site)
	if err != nil {
		l.Error(req.Site, " prepare error:", err)
		return
	}

	defer func() {
		_ = db.Close()
		// 执行 PRAGMA wal_checkpoint;
		// 更积极的模式可以使用 PRAGMA wal_checkpoint(TRUNCATE);
		// TRUNCATE 选项会在检查点完成后将WAL文件截断回0字节。
		_, err = dbLocal.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`)
		if err != nil {
			log.Printf("sqlite执行检查点失败: %v", err)
		} else {
			log.Println("sqlite检查点执行成功")
		}
		_ = dbLocal.Close()
	}()

	if req.TestDataSource {
		return
	}

	if req.Import {
		l.Info(req.Site, " start import...")
		impCfg := importer.Config{
			Ctx:      l.ctx,
			Db:       db,
			DbLocal:  dbLocal,
			Site:     req.Site,
			Debug:    req.Debug,
			AppConf:  l.svcCtx.Config,
			SiteConf: siteConf,
		}
		imp := importer.NewImporter(&impCfg)
		err = imp.Run()
		if err != nil {
			l.Error(req.Site, " import error:", err)
			return
		}
	}

	var successRate uint64
	conf := export.ExporterConfig{
		Ctx:      l.ctx,
		AppConf:  l.svcCtx.Config,
		SiteConf: siteConf,
		DbLocal:  dbLocal,
		Debug:    req.Debug}
	exp := export.NewExporter(&conf)
	if req.Export {
		l.Info(req.Site, " start export...")
		successRate, err = exp.Run()
		if err != nil {
			l.Error(req.Site, " export run error:", err)
			return
		}
		///成功率80%才做alias
		if successRate < 80 {
			l.Error(req.Site, "success rate less than 80%: ", successRate, "%")
			err = fmt.Errorf("fail: success rate (%d%%) less than 80%%, do not change alias", successRate)
			return
		}
		if req.Alias {
			//alias es index
			l.Info(req.Site, " successRate:", successRate, ", start alias ...")
			err = exp.Alias()
			if err != nil {
				l.Error(req.Site, " alias error:", err)
				return
			}
		} else {
			l.Info(req.Site, " successRate:", successRate, ", do not alias.")
		}
	} else {
		l.Info(req.Site, " do not export.")
	}

	l.Info(req.Site, " done...")

	resp = &types.Response{Message: "Done"}
	return
}
