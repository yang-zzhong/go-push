package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/dev-mockingbird/logf"
)

type httpBroker struct {
	storage Storage
	logger  logf.Logfer
}

type Resp struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data,omitempty"`
}

const (
	codeInvalidParams = "invalid.params"
	codeServerError   = "error.server"
	codeOK            = "ok"
)

func message(code, message string) Resp {
	return Resp{
		Code:    code,
		Message: message,
	}
}

func NewHTTPHandler(s Storage, logger logf.Logfer) http.Handler {
	return httpBroker{storage: s, logger: logger}
}

func (b httpBroker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Method", "POST")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	if req.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch req.URL.Path {
	case "/subscribe":
		b.subscribe(req, w)
	case "/unsubscribe":
		b.unsubscribe(w, req)
	case "/push":
		b.push(req, w)
	}
}

func (b httpBroker) unsubscribe(w http.ResponseWriter, req *http.Request) {
	var data struct {
		Topic      string `json:"topic"`
		Subscriber string `json:"subscriber"`
	}
	if err := b.readParams(req, &data); err != nil {
		b.writeResp(req, w, message(codeInvalidParams, err.Error()))
		return
	}
	q := GetQueue(data.Topic, b.storage, false)
	q.Unsubscribe(data.Subscriber)
	b.writeResp(req, w, message(codeOK, "ok"))
}

func (b httpBroker) push(req *http.Request, w http.ResponseWriter) {
	var body struct {
		Topic      string   `json:"topic"`
		Body       []string `json:"body"`
		AutoCreate bool     `json:"auto_create"`
	}
	if err := b.readParams(req, &body); err != nil {
		b.writeResp(req, w, message(codeInvalidParams, err.Error()))
		return
	}
	q := GetQueue(body.Topic, b.storage, body.AutoCreate)
	data := make([][]byte, len(body.Body))
	for i, d := range body.Body {
		data[i] = []byte(d)
	}
	if err := q.Add(context.Background(), data...); err != nil {
		return
	}
	b.writeJson(w, message(codeOK, ""))
}

func (b httpBroker) readParams(req *http.Request, data any) error {
	contentType := req.Header.Get("Content-Type")
	switch contentType {
	case "application/json", "":
		if err := json.NewDecoder(req.Body).Decode(data); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported content type [%s]", contentType)
	}
	return nil
}

func (b httpBroker) writeResp(_ *http.Request, w http.ResponseWriter, resp Resp) {
	b.writeJson(w, resp)
}

func (b httpBroker) subscribeParams(req *http.Request) (
	topic, subscriber string,
	offset int,
	batchSize int,
	autoCreate bool,
	err error,
) {
	topic = req.FormValue("topic")
	subscriber = req.FormValue("subscriber")
	offsetStr := req.FormValue("offset")
	batchSizeStr := req.FormValue("batch_size")
	offset = 0
	if offsetStr != "" {
		if offset, err = strconv.Atoi(offsetStr); err != nil {
			err = fmt.Errorf("parse offset: %w", err)
			return
		}
	}
	batchSize = 20
	if batchSizeStr != "" {
		if batchSize, err = strconv.Atoi(batchSizeStr); err != nil {
			err = fmt.Errorf("parse batch size: %w", err)
			return
		}
	}
	if subscriber == "" {
		err = errors.New("subscriber should not be empty")
		return
	}
	if topic == "" {
		err = errors.New("topic should not be empty")
		return
	}
	ac := req.FormValue("auto_create")
	autoCreate = ac != "" && ac != "0"
	return
}

func (b httpBroker) subscribe(req *http.Request, w http.ResponseWriter) {
	topic, subscriber, offset, batchSize, autoCreate, err := b.subscribeParams(req)
	if err != nil {
		b.writeResp(req, w, message(codeInvalidParams, err.Error()))
		return
	}
	q := GetQueue(topic, b.storage, autoCreate)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	err = q.Subscribe(
		req.Context(),
		subscriber,
		int64(offset),
		batchSize,
		func(d [][]byte, startOffset int64) error {
			bs, err := json.Marshal(struct {
				Data        []string `json:"data"`
				StartOffset int64    `json:"start_offset"`
			}{
				Data: func() []string {
					ret := make([]string, len(d))
					for i, v := range d {
						ret[i] = string(v)
					}
					return ret
				}(),
				StartOffset: startOffset,
			})
			if err != nil {
				b.logger.Logf(logf.Error, "marshal data: %s", err.Error())
				return nil
			}
			if _, err = w.Write(append(append([]byte("data:"), bs...), []byte("\n\n")...)); err != nil {
				b.logger.Logf(logf.Error, "write data: %s", err.Error())
				return nil
			}
			if err := rc.Flush(); err != nil {
				b.logger.Logf(logf.Error, "flush: %s", err.Error())
				return nil
			}
			return nil
		})
	if err != nil {
		b.logger.Logf(logf.Error, "subscribe: %s", err.Error())
	}
}

func (b httpBroker) writeJson(w http.ResponseWriter, data Resp) {
	w.Header().Set("Content-Type", "application/json")
	bs, err := json.Marshal(data)
	if err != nil {
		b.logger.Logf(logf.Error, "marshal json: %s", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(bs); err != nil {
		b.logger.Logf(logf.Error, "write json: %s", err.Error())
	}
}
