package svc

import (
	"testing"

	"sqlsyncify/internal/config"
)

func TestMySQLConfigKeepsPasswordOutOfParameters(t *testing.T) {
	cfg, err := mysqlConfig(config.DataSource{
		Host:     "db.internal",
		Port:     3306,
		Dbname:   "wordpress",
		Username: "wp",
		Password: "p@ss/word&multiStatements=true",
		TimeZone: "Local",
		Driver:   "mysql",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MultiStatements {
		t.Fatal("password must not enable multiStatements")
	}
	if cfg.Passwd != "p@ss/word&multiStatements=true" || cfg.DBName != "wordpress" || cfg.Addr != "db.internal:3306" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.Params["charset"] != "utf8mb4" {
		t.Fatalf("charset = %q", cfg.Params["charset"])
	}
}

func TestMySQLConfigRejectsDriverAndTraversal(t *testing.T) {
	_, err := mysqlConfig(config.DataSource{
		Host: "127.0.0.1", Port: 3306, Dbname: "wordpress", Username: "wp", Driver: "sqlite3",
	})
	if err == nil {
		t.Fatal("non-mysql driver must be rejected")
	}
	_, err = mysqlConfig(config.DataSource{
		Host: "127.0.0.1", Port: 3306, Dbname: "../secret", Username: "wp",
	})
	if err == nil {
		t.Fatal("database name with a path separator must be rejected")
	}
}
