package push

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/dev-mockingbird/logf"
	"github.com/r3labs/sse/v2"
)

type HTTPClient struct {
	Endpoint      string
	OffsetStorage OffsetStorage
	Underlying    http.Client
	logf.Logfer
	init          sync.Once
	subscribeLock sync.Mutex
}

func (c *HTTPClient) Subscribe(ctx context.Context, topic string, subscriber string, handle SubscribeHandler) error {
	if err := c.doInit(); err != nil {
		return err
	}
	c.subscribeLock.Lock()
	defer c.subscribeLock.Unlock()
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return err
	}
	var offset int64
	if err := c.OffsetStorage.GetOffset(ctx, topic, &offset); err != nil {
		return err
	}
	u.Path = fmt.Sprintf("/%s/subscribe", topic)
	q := u.Query()
	q.Set("subscriber", subscriber)
	q.Set("offset", fmt.Sprintf("%d", offset))
	u.RawQuery = q.Encode()
	client := sse.NewClient(u.String())
	return client.SubscribeWithContext(ctx, subscriber, func(msg *sse.Event) {
		var e SubMessage
		if err := json.Unmarshal(msg.Data, &e); err != nil {
			c.Logf(logf.Error, "subscribe: unmarshal data: %s", err.Error())
			return
		}
		offset = handle(e)
		if err := c.OffsetStorage.SetOffset(ctx, topic, offset); err != nil {
			c.Logf(logf.Error, "subscribe: set offset: %s", err.Error())
		}
	})
}

func (c *HTTPClient) Push(topic string, data [][]byte) error {
	if err := c.doInit(); err != nil {
		return err
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("/%s/push", topic)
	body := struct {
		Body       []string `json:"body"`
		AutoCreate bool     `json:"auto_create"`
	}{
		Body:       make([]string, len(data)),
		AutoCreate: true,
	}
	for i, v := range data {
		body.Body[i] = string(v)
	}
	bs, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(bs))
	if err != nil {
		return err
	}
	_, err = c.Underlying.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (c *HTTPClient) doInit() error {
	var err error
	c.init.Do(func() {
		if c.Endpoint == "" {
			err = fmt.Errorf("invalid endpoint [%s]", err.Error())
			return
		}
		if c.Logfer == nil {
			c.Logfer = logf.New()
		}
		if c.OffsetStorage == nil {
			c.OffsetStorage = NewMemoryOffsetStorage()
		}
	})
	return err
}
