package push

import (
	"context"
	"encoding/json"
	"net/http"
)

// POST /create  {"name": string}
// POST /push {"name": string, "item": any}
// POST /get {"name": string}
// POST /delete {"name": string}

type httpBroker struct {
	storage Storage
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

func data(code string, data any) Resp {
	return Resp{
		Code: code,
		Data: data,
	}
}

func NewHTTPHandler(s Storage) http.Handler {
	return httpBroker{storage: s}
}

func (b httpBroker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/subscribe":
		b.subscribe(req, w)
	case "/push":
		b.push(req, w)
	}
}

func (b httpBroker) push(req *http.Request, w http.ResponseWriter) {
	contentType := req.Header.Get("Content-Type")
	var body struct {
		Topic      string   `json:"topic"`
		Body       [][]byte `json:"body"`
		AutoCreate bool     `json:"auto_create"`
	}
	switch contentType {
	case "application/json", "":
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			b.writeJson(w, message(codeInvalidParams, err.Error()))
			return
		}
	}
	q := NewQueue(body.Topic, b.storage, body.AutoCreate)
	if err := q.Add(context.Background(), body.Body...); err != nil {
		b.writeJson(w, message(codeServerError, err.Error()))
		return
	}
	b.writeJson(w, message(codeOK, ""))
}

func (b httpBroker) subscribe(req *http.Request, w http.ResponseWriter) {
	contentType := req.Header.Get("Content-Type")
	var body struct {
		Topic      string `json:"topic"`
		Subscriber string `json:"subscriber"`
		Offset     int64  `json:"offset"`
		Proto      string `json:"proto"`
		AutoCreate bool   `json:"auto_create"`
		BatchSize  int    `json:"batch_size"`
	}
	switch contentType {
	case "application/json", "":
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			b.writeJson(w, message(codeInvalidParams, err.Error()))
			return
		}
	}
	q := NewQueue(body.Topic, b.storage, body.AutoCreate)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Connection", "keep-alive")
	q.Subscribe(
		req.Context(),
		body.Subscriber,
		body.Offset,
		body.BatchSize,
		func(d [][]byte, startOffset int64) error {
			bs, err := json.Marshal(struct {
				Data        [][]byte `json:"data"`
				StartOffset int64    `json:"start_offset"`
			}{
				Data:        d,
				StartOffset: startOffset,
			})
			if err != nil {
				// log
			}
			w.Write(bs)
			return nil
		})
}

func (httpBroker) writeJson(w http.ResponseWriter, data Resp) {
	w.Header().Set("Content-Type", "application/json")
	bs, err := json.Marshal(data)
	if err != nil {
		// log
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(bs); err != nil {
		// log
	}
}
