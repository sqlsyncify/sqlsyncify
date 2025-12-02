package importer

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/svc"
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestImportDb(t *testing.T) {

	_ = os.Chdir("../../../")
	t.Log(os.Getwd())
	var configFile = flag.String("f", "etc/sqlsyncify-api.yaml", "the config file")
	var appConf config.Config
	conf.MustLoad(*configFile, &appConf)
	cfg, _ := json.Marshal(appConf)
	log.Println(string(cfg))

	site := "local"

	siteConf, err := svc.NewSiteConf(site)
	if err != nil {
		t.Fatal(site, " failed to load site conf: ", err)
	}
	conn, err := svc.NewDbConn(siteConf.DataSource)
	if err != nil {
		t.Fatalf("%s failed to connect DataSource: %v", site, err)
	}
	dbLocal, err := svc.NewSqliteConn(site)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = dbLocal.Close()
		_ = conn.Close()
	}()

	impCfg := &Config{
		Ctx:      context.Background(),
		Db:       conn,
		DbLocal:  dbLocal,
		Site:     site,
		Debug:    true,
		AppConf:  appConf,
		SiteConf: siteConf,
	}
	imp := NewImporter(impCfg)

	err = imp.Run()
	if err != nil {
		t.Errorf("imp.Run  error = %v", err)
	}
}
