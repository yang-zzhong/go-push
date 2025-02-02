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
		items[i] = &DBItem{
			Data: bs,
		}
	}
	err := q.DB.Table("q_" + name).Create(items).Error
	if err != nil && err.Error() == "no such table: q_"+name {
		return ErrQueueNotFound
	}
	return err
}

func (q dbstorage) Create(ctx context.Context, name string) error {
	return q.DB.Table("q_" + name).AutoMigrate(&DBItem{})
}

func (q dbstorage) Get(ctx context.Context, name string, offset, limit int64) ([][]byte, error) {
	data := [][]byte{}
	err := q.DB.Table("q_"+name).
		Where("offset >= ?", offset).
		Limit(int(limit)).
		Order("offset asc").Pluck("data", &data).Error
	if err != nil && err.Error() == "no such table: q_"+name {
		return nil, ErrQueueNotFound
	}
	return data, err
}
