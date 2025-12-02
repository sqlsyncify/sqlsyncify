package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/handler"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
	_ "go.uber.org/automaxprocs"
)

var configFile = flag.String("f", "etc/sqlsyncify-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	cfg, _ := json.Marshal(c)
	log.Println(string(cfg))

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)

	handler.RegisterHandlers(server, ctx)

	debug := os.Getenv("APP_DEBUG")
	if debug == "1" || debug == "true" {
		go func() {
			log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
		}()
	}
	log.Printf("当前系统的 CPU 核心数为: %d，当前 GOMAXPROCS 为 %d\n", runtime.NumCPU(), runtime.GOMAXPROCS(0))
	log.Printf("[INFO] Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
