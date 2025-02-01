package push

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type DBItem struct {
	Offset    int64     `gorm:"column:offset;type:BIGINT;primarykey,auto_increment"`
	Data      []byte    `gorm:"column:data;type:LONGTEXT"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreate"`
	name      string
}

func (i DBItem) TableName() string {
	return "q_" + i.name
}

func NewDBItem(name string) *DBItem {
	return &DBItem{name: name}
}

type dbstorage struct {
	DB *gorm.DB
}

func NewDBStorage(db *gorm.DB) Storage {
	return dbstorage{DB: db}
}

func (q dbstorage) Add(ctx context.Context, name string, data [][]byte) error {
	if len(data) == 0 {
		return nil
	}
	items := make([]*DBItem, len(data))
	for i, bs := range data {
		item := NewDBItem(name)
		item.Data = bs
		items[i] = item
	}
	return q.DB.Create(items).Error
}

func (q dbstorage) Create(ctx context.Context, name string) error {
	return q.DB.AutoMigrate(NewDBItem(name))
}

func (q dbstorage) Get(ctx context.Context, name string, offset, limit int64) ([][]byte, error) {
	data := [][]byte{}
	return data, q.DB.Model("q_"+name).
		Where("offset >= ?", offset).
		Limit(int(limit)).
		Order("offset asc").Pluck("data", &data).Error
}
