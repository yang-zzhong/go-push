package push

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"
)

type DBItem struct {
	Offset    int64     `gorm:"column:offset;type:BIGINT;primarykey"`
	Data      []byte    `gorm:"column:data;type:LONGTEXT"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreate"`
}

type dbstorage struct {
	DB    *gorm.DB
	locks map[string]*sync.Mutex
	lock  sync.RWMutex
}

func NewDBStorage(db *gorm.DB) Storage {
	return &dbstorage{DB: db}
}

func (q *dbstorage) Add(ctx context.Context, name string, data [][]byte) error {
	if len(data) == 0 {
		return nil
	}
	q.lock.RLock()
	locker, ok := q.locks[name]
	q.lock.RUnlock()
	if !ok {
		locker = &sync.Mutex{}
		q.lock.Lock()
		if q.locks == nil {
			q.locks = make(map[string]*sync.Mutex)
		}
		q.locks[name] = locker
		q.lock.Unlock()
	}
	locker.Lock()
	defer locker.Unlock()
	var total int64
	if err := q.DB.Table("q_" + name).Count(&total).Error; err != nil {
		if err.Error() == "no such table: q_"+name {
			return ErrQueueNotFound
		}
		return fmt.Errorf("get offset: %w", err)
	}
	items := make([]*DBItem, len(data))
	for i, bs := range data {
		items[i] = &DBItem{
			Data:   bs,
			Offset: total + int64(i) + 1,
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
