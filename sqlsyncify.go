package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/handler"
	"sqlsyncify/internal/svc"
	"strings"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
	_ "go.uber.org/automaxprocs"
)

var configFile = flag.String("f", "etc/sqlsyncify-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	log.Printf("[INFO] config loaded, host=%s port=%d\n", c.Host, c.Port)
	if c.AdminToken() == "" {
		log.Println("[WARN] admin API is disabled until SQLSYNCIFY_AUTH_TOKEN or AuthToken is set")
	}

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)

	handler.RegisterHandlers(server, ctx)

	debug := os.Getenv("APP_DEBUG")
	if debug == "1" || strings.EqualFold(debug, "true") {
		go func() {
			// Loopback only. pprof can dump memory, including indexed documents and credentials.
			addr := "127.0.0.1:6060"
			log.Printf("[INFO] pprof listening on %s\n", addr)
			log.Println(http.ListenAndServe(addr, nil))
		}()
	}
	log.Printf("当前系统的 CPU 核心数为: %d，当前 GOMAXPROCS 为 %d\n", runtime.NumCPU(), runtime.GOMAXPROCS(0))
	log.Printf("[INFO] Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
