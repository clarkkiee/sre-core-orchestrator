BINARY_NAME := orchestrator-agent
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -X main.Version=$(VERSION) -s -w

.PHONY: build build-linux clean

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) .

build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME)-linux-amd64 .

clean:
	rm -rf bin/
