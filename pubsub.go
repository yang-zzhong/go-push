package push

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/dev-mockingbird/events"
)

type Publisher interface {
	Publish(ctx context.Context, e ...*events.Event) error
}

type Subscriber interface {
	Subscribe(ctx context.Context, handler func(e *events.Event) error) error
	Unsubscribe(ctx context.Context) error
}

type SubscriberFactory interface {
	CreateSubscriber(consumer string) Subscriber
}

type PubSub interface {
	Topic() string
	Publisher
	SubscriberFactory
}

type pubsub struct {
	topic string
	*HTTPClient
}

type subscriber struct {
	topic    string
	consumer string
	*HTTPClient
}

func NewPubSub(topic string, c *HTTPClient) PubSub {
	return &pubsub{topic: topic, HTTPClient: c}
}

// Subscribe implements Subscriber.
func (s *subscriber) Subscribe(ctx context.Context, handler func(e *events.Event) error) error {
	return s.HTTPClient.Subscribe(ctx, s.topic, s.consumer, func(msg SubMessage) (currentOffset int64) {
		for i, m := range msg.Data {
			var e events.Event
			if err := json.NewDecoder(strings.NewReader(m)).Decode(&e); err != nil {
				// handle error
				return int64(msg.StartOffset + i)
			}
			if err := handler(&e); err != nil {
				// handle error
				return int64(msg.StartOffset + i)
			}
		}
		return int64(msg.StartOffset + len(msg.Data))
	})
}

// Unsubscribe implements Subscriber.
func (s *subscriber) Unsubscribe(ctx context.Context) error {
	return nil
}

// CreateSubscriber implements PubSub.
func (p *pubsub) CreateSubscriber(consumer string) Subscriber {
	return &subscriber{topic: p.topic, HTTPClient: p.HTTPClient, consumer: consumer}
}

// Publish implements PubSub.
func (p *pubsub) Publish(ctx context.Context, es ...*events.Event) error {
	data := make([][]byte, len(es))
	for i, e := range es {
		bs, err := json.Marshal(e)
		if err != nil {
			return err
		}
		data[i] = bs
	}
	return p.HTTPClient.Push(p.topic, data)
}

// Topic implements PubSub.
func (p *pubsub) Topic() string {
	return p.topic
}

var _ PubSub = &pubsub{}
