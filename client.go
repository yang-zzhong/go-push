package push

import "context"

type SubMessage struct {
	StartOffset int      `json:"start_offset"`
	Data        []string `json:"data"`
}

type SubscribeHandler func(msg SubMessage) (currentOffset int64)

type OffsetStorage interface {
	SetOffset(ctx context.Context, topic string, offset int64) error
	GetOffset(ctx context.Context, topic string, offset *int64) error
}

type memoryOffsetStorage struct {
	data map[string]int64
}

func NewMemoryOffsetStorage() OffsetStorage {
	return &memoryOffsetStorage{
		data: make(map[string]int64),
	}
}

func (m *memoryOffsetStorage) SetOffset(_ context.Context, topic string, offset int64) error {
	m.data[topic] = offset
	return nil
}

func (m *memoryOffsetStorage) GetOffset(_ context.Context, topic string, offset *int64) error {
	if o, ok := m.data[topic]; ok {
		*offset = o
	}
	*offset = 0
	return nil
}
