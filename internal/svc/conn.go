package svc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sqlsyncify/internal/config"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/conf"
)

func NewSiteConf(site string) (*config.SiteConfig, error) {
	// 用于不同的站点配置不同的数据源
	ymlFile := fmt.Sprintf("./etc/sites/%s/%s.yaml", site, site)
	log.Println("load site:", ymlFile)
	var cfg config.SiteConfig
	err := conf.Load(ymlFile, &cfg)
	if err != nil {
		return nil, err
	}
	cfgJson, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	log.Println("SiteConfig", string(cfgJson))
	// 未设置别名时，用索引名前缀做别名
	if len(cfg.AliasName) == 0 {
		cfg.AliasName = cfg.IndexName
	}

	return &cfg, nil
}

func NewDbConn(ds string) (*sql.DB, error) {
	// 用于不同的数据源
	ymlFile := fmt.Sprintf("etc/datasources/%s.yaml", ds)
	log.Println("load datasource:", ymlFile)
	var dsConf config.DataSource
	err := conf.Load(ymlFile, &dsConf)
	if err != nil {
		return nil, err
	}

	//TimeZone = Asia/Shanghai
	dsConf.TimeZone = strings.ReplaceAll(dsConf.TimeZone, "/", "%2F")
	// Connect to MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=False&loc=%s", dsConf.Username, dsConf.Password, dsConf.Host, dsConf.Port, dsConf.Dbname, dsConf.TimeZone)
	log.Println("DSN", dsn)
	db, err := sql.Open(dsConf.Driver, dsn)
	if err != nil {
		return nil, err
	}
	if len(dsConf.InitSql) > 0 {
		_, err = db.Exec(dsConf.InitSql)
		log.Println("InitSql ", dsConf.InitSql)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}
