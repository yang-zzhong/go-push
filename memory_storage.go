package push

import (
	"context"
	"sync"
)

type memorystorage struct {
	data map[string][][]byte
	lock sync.RWMutex
}

func NewMemoryStorage() Storage {
	return &memorystorage{data: make(map[string][][]byte)}
}

func (q *memorystorage) Create(ctx context.Context, name string) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.data[name]; !ok {
		q.data[name] = [][]byte{}
	}
	return nil
}

func (q *memorystorage) Add(ctx context.Context, name string, data [][]byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.data[name]; !ok {
		return ErrQueueNotFound
	}
	q.data[name] = append(q.data[name], data...)
	return nil
}

func (q *memorystorage) Get(ctx context.Context, name string, offset, limit int64) ([][]byte, error) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	if _, ok := q.data[name]; !ok {
		return nil, ErrQueueNotFound
	}
	start := int(offset)
	if start >= len(q.data[name]) {
		return nil, nil
	}
	end := start + int(limit)
	if end > len(q.data[name]) {
		end = len(q.data[name])
	}
	return q.data[name][start:end], nil
}
