package push_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yang-zzhong/go-push"
)

func TestQueue(t *testing.T) {
	q := push.NewQueue("hello", push.NewMemoryStorage(), true)
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			if err := q.Add(ctx, []byte(fmt.Sprintf("%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			q.Unsubscribe(fmt.Sprintf("consumer: %d", i))
		}
	}()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := q.Subscribe(ctx, fmt.Sprintf("consumer: %d", i), 0, 100, func(data [][]byte, startOffset int64) error {
				for o, item := range data {
					fmt.Printf("consumer: %d, offset: %d: data: %s\n", i, startOffset+int64(o), item)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()
}

func TestQueue_batch(t *testing.T) {
	q := push.NewQueue("hello", push.NewMemoryStorage(), true)
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		data := [][]byte{}
		for i := 0; i < 10; i++ {
			data = append(data, []byte(fmt.Sprintf("%d", i)))
		}
		if err := q.Add(ctx, data...); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 100)
		for i := 0; i < 10; i++ {
			q.Unsubscribe(fmt.Sprintf("consumer: %d", i))
		}
	}()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := q.Subscribe(ctx, fmt.Sprintf("consumer: %d", i), 0, 100, func(data [][]byte, startOffset int64) error {
				for o, item := range data {
					time.Sleep(time.Millisecond * 50)
					fmt.Printf("consumer: %d, offset: %d: data: %s\n", i, startOffset+int64(o), item)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()
}
