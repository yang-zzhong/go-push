VERSION ?= $(shell git describe --always --tags)
HUB ?= localhost:5001/mockingbird

.PHONY: init
init:
	@go install golang.org/x/text/cmd/gotext@latest

.PHONY: update
update:
	@go get -u

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: test
test:
	@go test -v ./... -cover

.PHONY: locale
locale: init
	gotext -srclang=en update -out=catalog.go -lang=en,zh

.PHONY: build
build: 
	@go build -o build/push cmd/push/main.go

.PHONY: clear
clear:
	rm -rf build

.PHONY: image
image:
	@docker build -t ${HUB}_push:$(VERSION) -f Dockerfile .

.PHONY: push-image
push-image: image
	@docker push ${HUB}_push:$(VERSION)