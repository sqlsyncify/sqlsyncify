package export

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"regexp"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/svc"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestAlias(t *testing.T) {
	_ = os.Chdir("../../../")

	t.Log(os.Getwd())
	var configFile = flag.String("f", "etc/sqlsyncify-api.yaml", "the config file")
	var appConf config.Config
	conf.MustLoad(*configFile, &appConf)
	cfg, _ := json.Marshal(appConf)
	log.Println(string(cfg))

	site := "wordpress"
	// 用于不同的站点配置不同的数据源

	siteConf, err := svc.NewSiteConf(site)
	if err != nil {
		t.Fatal(site, " failed to load site conf: ", err)
	}
	db, err := svc.NewDbConn(siteConf.DataSource)
	if err != nil {
		t.Fatalf("%s failed to connect to DataSource: %v", site, err)
	}
	if err != nil {
		t.Error(site, " failed to connect to DataSource: ", err)
		return
	}
	//prepare sqlite
	dbLocal, err := svc.NewSqliteConn(site)
	if err != nil {
		t.Error(site, " prepare error:", err)
		return
	}
	defer func() {
		db.Close()
		dbLocal.Close()
	}()
	conf := &ExporterConfig{Ctx: context.Background(), AppConf: appConf, SiteConf: siteConf, DbLocal: dbLocal, Debug: true}
	exp := NewExporter(conf)

	percent, err1 := exp.Run()
	if err1 != nil {
		t.Error(site, " prepare error:", err)
		return
	}
	t.Log("percent", percent)

	if percent < 80 {
		t.Fatalf("export percent < 80%% : %d%%", percent)
	}
	err = exp.Alias()
	if err != nil {
		t.Fatal(site, " prepare error:", err)
	}
}

func TestRegexp(t *testing.T) {
	// 编译正则表达式
	re := regexp.MustCompile(`\d+`)

	// 待匹配字符串
	str := "abc123def456"

	// 查找第一个匹配项
	match := re.FindString(str)
	t.Log(match) // 输出：123

	str = "test_20240912171638"
	match = re.FindString(str)
	t.Log(match)

	re2 := regexp.MustCompile(`test_\d+`)
	match = re2.FindString(str)
	t.Log(match)
}

func TestDateTime(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	now := time.Now().In(loc)
	t.Log(now.Format("2006-01-02 15:04:05"))
}

func TestCutStr(t *testing.T) {
	sqlStr := " \tselect \nfrom t"
	prefix := strings.Trim(sqlStr, " \t\n\r")[:6]
	prefix = strings.ToUpper(prefix)
	if prefix != "SELECT" {
		t.Error("not select")
	}
	t.Log(prefix, "OK")
}
