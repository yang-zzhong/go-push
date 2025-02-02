package push

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

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
	for i, d := range data {
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

func (b httpBroker) subscribe(req *http.Request, w http.ResponseWriter) {
	topic := req.FormValue("topic")
	subscriber := req.FormValue("subscriber")
	offsetStr := req.FormValue("offset")
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		b.writeJson(w, message(codeInvalidParams, err.Error()))
		return
	}
	autoCreate := req.FormValue("auto_create")
	q := GetQueue(topic, b.storage, autoCreate != "" && autoCreate != "0")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Connection", "keep-alive")
	q.Subscribe(
		req.Context(),
		subscriber,
		int64(offset),
		20,
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
