package push_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/tj/assert"
	"github.com/yang-zzhong/go-push"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func dialector(db *sql.DB) gorm.Dialector {
	return mysql.New(mysql.Config{
		Conn:                      db,
		DriverName:                "mysql",
		SkipInitializeWithVersion: true,
	})
}

func TestCreateTable(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	func() {
		querySql := "SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE \\? ORDER BY SCHEMA_NAME=\\? DESC,SCHEMA_NAME limit 1"
		mock.ExpectQuery(querySql).WithArgs("%", "").WillReturnRows(&sqlmock.Rows{})
		execSql := "CREATE TABLE `q_hello` \\(`offset` BIGINT,`data` LONGTEXT,`created_at` datetime\\(3\\) NULL\\)"
		mock.ExpectExec(execSql).WillReturnResult(sqlmock.NewResult(0, 0))
	}()
	s := push.NewDBStorage(gdb)
	ctx := context.Background()
	if err := s.Create(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, err)
}

func TestAdd(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	func() {
		mock.ExpectBegin()
		execSql := "INSERT INTO `q_hello` \\(`offset`,`data`,`created_at`\\) VALUES \\(\\?,\\?,\\?\\)"
		mock.ExpectExec(execSql).WithArgs(0, []byte("hello"), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	}()
	s := push.NewDBStorage(gdb)
	ctx := context.Background()
	if err := s.Add(ctx, "hello", [][]byte{[]byte("hello")}); err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, err)
}

func TestGet(t *testing.T) {
	db, mock, err := sqlmock.New() // mock sql.DB
	assert.Nil(t, err)
	defer db.Close()
	defer assert.Nil(t, mock.ExpectationsWereMet())
	gdb, err := gorm.Open(dialector(db)) // open gorm db
	assert.Nil(t, err)
	func() {
		execSql := "SELECT `data` FROM `q_hello` WHERE offset >= \\? ORDER BY offset asc LIMIT \\?"
		mock.ExpectQuery(execSql).WithArgs(0, 1).WillReturnRows(&sqlmock.Rows{})
	}()
	s := push.NewDBStorage(gdb)
	ctx := context.Background()
	if _, err := s.Get(ctx, "hello", 0, 1); err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, err)
}
