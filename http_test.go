package push_test

import (
	"net/http"
	"testing"

	"github.com/yang-zzhong/go-push"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestHTTPServer(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("test.db"))
	if err != nil {
		t.Fatal(err)
	}
	s := http.Server{
		Addr:    ":8081",
		Handler: push.NewHTTPHandler(push.NewDBStorage(db)),
	}
	s.ListenAndServe()
}
