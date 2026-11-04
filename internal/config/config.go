package config

import (
	"os"
	"strings"

	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	AppHost string
	// AuthToken protects /sync and /clean. SQLSYNCIFY_AUTH_TOKEN overrides this
	// value when set. When both are empty those routes are denied.
	AuthToken string `json:",optional"`
}

// AdminToken is the shared secret for state-changing routes.
func (c Config) AdminToken() string {
	if v := strings.TrimSpace(os.Getenv("SQLSYNCIFY_AUTH_TOKEN")); v != "" {
		return v
	}
	return strings.TrimSpace(c.AuthToken)
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
