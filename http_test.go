package push_test

import (
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/dev-mockingbird/logf"
	"github.com/yang-zzhong/go-push"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestHTTPServer(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{Logger: logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{LogLevel: logger.Info}),
	})
	if err != nil {
		t.Fatal(err)
	}
	s := http.Server{
		Addr:    ":8081",
		Handler: push.NewHTTPHandler(push.NewDBStorage(db), logf.New()),
	}
	s.ListenAndServe()
}
