SERVICE_NAME := social_service
GO_BUILD_FLAGS := -trimpath
BINARY := bin/server

.PHONY: install test build run lint migrate-up migrate-down cover

install:
	go mod download

test:
	go test ./... -race -count=1 -parallel 4

cover:
	go test ./... -race -count=1 -parallel 4 -coverpkg=./internal/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

build:
	CGO_ENABLED=0 go build $(GO_BUILD_FLAGS) -ldflags="-s -w" -o $(BINARY) ./cmd/server

run:
	go run ./cmd/server

lint:
	golangci-lint run ./...

migrate-up:
	go run ./cmd/migrate -direction up

migrate-down:
	go run ./cmd/migrate -direction down
