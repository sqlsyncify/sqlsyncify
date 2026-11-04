package svc

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/utils"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/conf"
)

const maxInitSQL = 64 * 1024

func NewSiteConf(site string) (*config.SiteConfig, error) {
	if !utils.CheckSiteFormat(site) {
		return nil, errors.New("invalid site")
	}
	ymlFile, err := utils.ResolveUnder("etc/sites", site, site+".yaml")
	if err != nil {
		return nil, err
	}
	log.Println("load site:", site)
	var cfg config.SiteConfig
	if err = conf.Load(ymlFile, &cfg); err != nil {
		return nil, err
	}
	if cfg.Site == "" {
		cfg.Site = site
	}
	if !utils.CheckSiteFormat(cfg.Site) {
		return nil, errors.New("invalid site in config")
	}
	if !utils.SafeDatasourceName(cfg.DataSource) {
		return nil, errors.New("invalid datasource in config")
	}
	if !utils.SafeIndexToken(cfg.IndexName) {
		return nil, errors.New("invalid index name")
	}
	if cfg.AliasName != "" && !utils.SafeIndexToken(cfg.AliasName) {
		return nil, errors.New("invalid alias name")
	}
	if cfg.Lang != "" && !utils.CheckLangFormat(cfg.Lang) {
		return nil, errors.New("invalid lang")
	}
	if _, err = utils.ParseESAddresses(cfg.EsCluster); err != nil {
		return nil, err
	}
	if len(cfg.AliasName) == 0 {
		cfg.AliasName = cfg.IndexName
	}
	// Do not log EsApiKey or the raw YAML.
	log.Printf("site=%s es=%s index=%s alias=%s", cfg.Site, cfg.EsVersion, cfg.IndexName, cfg.AliasName)
	return &cfg, nil
}

func NewDbConn(ds string) (*sql.DB, error) {
	if !utils.SafeDatasourceName(ds) {
		return nil, errors.New("invalid datasource name")
	}
	ymlFile, err := utils.ResolveUnder("etc/datasources", ds+".yaml")
	if err != nil {
		return nil, err
	}
	log.Println("load datasource:", ds)
	var dsConf config.DataSource
	if err = conf.Load(ymlFile, &dsConf); err != nil {
		return nil, err
	}
	mysqlCfg, err := mysqlConfig(dsConf)
	if err != nil {
		return nil, err
	}
	log.Printf("connect mysql host=%s port=%d db=%s user=%s", dsConf.Host, dsConf.Port, dsConf.Dbname, dsConf.Username)
	// Open from Config so the password is never parsed as a DSN.
	connector, err := mysql.NewConnector(mysqlCfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	if len(dsConf.InitSql) > 0 {
		if len(dsConf.InitSql) > maxInitSQL {
			_ = db.Close()
			return nil, errors.New("datasource InitSql is too long")
		}
		if _, err = db.Exec(dsConf.InitSql); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

// mysqlConfig builds a driver config. Passwords are kept as fields so characters
// such as '@' or '&' cannot change the host or enable extra DSN parameters.
func mysqlConfig(dsConf config.DataSource) (*mysql.Config, error) {
	if dsConf.Driver != "" && !strings.EqualFold(dsConf.Driver, "mysql") {
		return nil, fmt.Errorf("unsupported datasource driver %q", dsConf.Driver)
	}
	if dsConf.Host == "" || strings.ContainsAny(dsConf.Host, " \t\r\n/\\@") {
		return nil, errors.New("invalid datasource host")
	}
	if dsConf.Port <= 0 || dsConf.Port > 65535 {
		return nil, errors.New("invalid datasource port")
	}
	if dsConf.Username == "" || strings.ContainsAny(dsConf.Username, " \t\r\n") {
		return nil, errors.New("invalid datasource user")
	}
	if dsConf.Dbname == "" || strings.ContainsAny(dsConf.Dbname, " \t\r\n/?&#\\") {
		return nil, errors.New("invalid database name")
	}
	cfg := mysql.NewConfig()
	cfg.User = dsConf.Username
	cfg.Passwd = dsConf.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(dsConf.Host, strconv.Itoa(dsConf.Port))
	cfg.DBName = dsConf.Dbname
	cfg.ParseTime = false
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	switch tz := dsConf.TimeZone; tz {
	case "":
		// Driver default (UTC). An empty loc= parameter is not a valid zone.
	case "Local":
		cfg.Loc = time.Local
	default:
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return nil, errors.New("invalid datasource timezone")
		}
		cfg.Loc = loc
	}
	cfg.MultiStatements = false
	return cfg, nil
}
