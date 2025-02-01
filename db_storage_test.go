package push_test

import (
	"context"
	"testing"

	"github.com/yang-zzhong/go-push"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDBStorage(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s := push.NewDBStorage(db)
	ctx := context.Background()
	if err := s.Create(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(ctx, "hello", [][]byte{[]byte("hello")}); err != nil {
		t.Fatal(err)
	}
}
