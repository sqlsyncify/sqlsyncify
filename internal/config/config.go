package config

import (
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	AppHost string
}

type DataSource struct {
	Host     string
	Port     int
	Dbname   string
	Username string
	Password string
	Driver   string
	TimeZone string
	InitSql  string
}

type SiteConfig struct {
	// 默认数据源，可用于同义词
	DataSource  string
	Site        string
	EsVersion   string
	EsCluster   string
	EsApiKey    string
	ImportLimit int
	IndexName   string
	AliasName   string `json:",optional"`
	Lang        string
	TimeZone    string
	DocTypeName string
	DocIdKey    string
}

func (c SiteConfig) EnabledImportLimit() bool {
	return c.ImportLimit > 0
}
