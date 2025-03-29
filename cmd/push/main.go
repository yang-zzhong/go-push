package main

import (
	"net/http"
	"os"

	"github.com/dev-mockingbird/logf"
	"github.com/yang-zzhong/go-push"
	"github.com/yang-zzhong/go-push/config"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	var cfg config.Config
	logger := logf.New()
	if err := config.ReadInput(&cfg, logger); err != nil {
		panic(err)
	}
	if cfg.Logpath != "" {
		f, err := os.OpenFile(cfg.Logpath, os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			panic("can't open log file: " + err.Error())
		}
		logger = logf.New(logf.CustomPrinter(logf.NewPrinter(f)))
	}
	var db *gorm.DB
	var err error
	switch cfg.DB.DBMS {
	case config.DBMysql:
		db, err = gorm.Open(mysql.Open(cfg.DB.DSN()))
	case config.DBPgsql:
		db, err = gorm.Open(postgres.Open(cfg.DB.DSN()))
	case config.DBSqlite:
		db, err = gorm.Open(sqlite.Open(cfg.DB.DSN()))
	default:
		panic("not support DBMS [" + cfg.DB.DBMS + "]")
	}
	if err != nil {
		panic("can't open DB: " + err.Error())
	}
	s := http.Server{
		Addr:    cfg.Http,
		Handler: push.NewHTTPHandler(push.NewDBStorage(db), logger),
	}
	logger.Logf(logf.Info, "start listen http on %s", cfg.Http)
	if err := s.ListenAndServe(); err != nil {
		logger.Logf(logf.Fatal, "listen: %s", err.Error())
	}
}
