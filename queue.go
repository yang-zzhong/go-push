package push

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yang-zzhong/go-pipeline"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	queue            = make(map[string]*Queue)
	queueLock        sync.RWMutex
)

type Storage interface {
	Add(ctx context.Context, name string, data [][]byte) error
	Get(ctx context.Context, name string, offset, limit int64) ([][]byte, error)
	Create(ctx context.Context, name string) error
}

type Queue struct {
	name        string
	storage     Storage
	autoCreate  bool
	sublock     sync.RWMutex
	subscribers map[string]chan struct{}
}

func GetQueue(name string, storage Storage, autoCreate bool) *Queue {
	queueLock.RLock()
	if q, ok := queue[name]; ok {
		queueLock.RUnlock()
		return q
	}
	queueLock.RUnlock()
	q := NewQueue(name, storage, autoCreate)
	queueLock.Lock()
	defer queueLock.Unlock()
	queue[name] = q
	return q
}

func NewQueue(name string, storage Storage, autoCreate bool) *Queue {
	return &Queue{
		name:        name,
		autoCreate:  autoCreate,
		storage:     storage,
		subscribers: make(map[string]chan struct{}),
	}
}

func (q *Queue) Add(ctx context.Context, data ...[]byte) error {
	if err := q.storage.Add(ctx, q.name, data); err != nil {
		if !errors.Is(err, ErrQueueNotFound) || !q.autoCreate {
			return err
		}
		if err := q.storage.Create(ctx, q.name); err != nil {
			return fmt.Errorf("add: %w", err)
		}
	}
	q.sublock.RLock()
	defer q.sublock.RUnlock()
	var wg sync.WaitGroup
	for _, c := range q.subscribers {
		wg.Add(1)
		go func(c chan struct{}) {
			c <- struct{}{}
			wg.Done()
		}(c)
	}
	wg.Wait()
	return nil
}

type dataWithOffset struct {
	data   []byte
	offset int64
}

func (q *Queue) Unsubscribe(name string) {
	q.sublock.Lock()
	defer q.sublock.Unlock()
	if ch, ok := q.subscribers[name]; ok {
		close(ch)
		delete(q.subscribers, name)
	}
}

func (q *Queue) Subscribe(
	ctx context.Context,
	name string,
	offset int64,
	batchSize int,
	consume func(data [][]byte, startOffset int64) error,
) error {
	q.sublock.Lock()
	if _, ok := q.subscribers[name]; ok {
		q.sublock.Unlock()
		return fmt.Errorf("subscriber [%s] is existed", name)
	}
	q.subscribers[name] = make(chan struct{}, 1)
	q.sublock.Unlock()
	if err := q.consume(ctx, &offset, batchSize, consume); err != nil {
		return err
	}
	for {
		q.sublock.RLock()
		ch, ok := q.subscribers[name]
		q.sublock.RUnlock()
		if !ok {
			return nil
		}
		if _, ok := <-ch; !ok {
			return nil
		}
		if err := q.consume(ctx, &offset, batchSize, consume); err != nil {
			return err
		}
	}
}

func (q *Queue) consume(
	ctx context.Context,
	offset *int64,
	batchSize int,
	consume func(data [][]byte, startOffset int64) error,
) error {
	p := pipeline.New1[dataWithOffset]()
	p.Start(func() ([]dataWithOffset, bool, error) {
		dt, err := q.storage.Get(ctx, q.name, *offset, 20)
		if err != nil {
			if !errors.Is(err, ErrQueueNotFound) || !q.autoCreate {
				return nil, false, err
			}
			if err := q.storage.Create(ctx, q.name); err != nil {
				return nil, false, fmt.Errorf("consume: %w", err)
			}
		}
		if len(dt) == 0 {
			return nil, true, nil
		}
		ret := make([]dataWithOffset, len(dt))
		for i, item := range dt {
			ret[i] = dataWithOffset{
				data:   item,
				offset: *offset + int64(i),
			}
		}
		*offset += int64(len(dt))
		return ret, false, nil
	}).Next1(func(data []dataWithOffset) ([]pipeline.E, error) {
		if len(data) == 0 {
			return nil, nil
		}
		ret := make([][]byte, len(data))
		for i, item := range data {
			ret[i] = item.data
		}
		return nil, consume(ret, data[0].offset)
	}, batchSize)
	return p.Do(ctx)
}
