package export

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

func TestAliasV5(t *testing.T) {
	_ = os.Chdir("../../../")
	t.Log(os.Getwd())
	var configFile = flag.String("f", "etc/sqlsyncify-api.yaml", "the config file")
	var appConf config.Config
	conf.MustLoad(*configFile, &appConf)
	cfg, _ := json.Marshal(appConf)
	log.Println(string(cfg))

	site := "wordpress"

	siteConf, err := svc.NewSiteConf(site)
	if err != nil {
		t.Fatal(site, " failed to load site conf: ", err)
	}
	db, err := svc.NewDbConn(siteConf.DataSource)
	if err != nil {
		t.Fatalf("%s failed to connect to DataSource: %v", site, err)
	}
	//prepare sqlite
	dbLocal, err := svc.NewSqliteConn(site)
	if err != nil {
		t.Fatalf(site, " prepare error:", err)
	}
	defer func() {
		_ = db.Close()
		_ = dbLocal.Close()
	}()
	cfgExp := &ExporterConfig{Ctx: context.Background(), AppConf: appConf, SiteConf: siteConf, DbLocal: dbLocal, Debug: true}
	exp := NewExporter(cfgExp)
	percent, err := exp.Run()
	if err != nil {
		t.Fatal(site, " prepare error:", err)
	}
	if percent < 80 {
		t.Fatalf("export percent < 80%% : %d%%", percent)
	}
	err = exp.Alias()
	if err != nil {
		t.Fatal(site, " prepare error:", err)
	}

}
