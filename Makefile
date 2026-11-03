.PHONY: help build test test-short cover lint run-sim run-gateway run-ingestor up down logs

help: ## List the commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

build: ## Build every program into ./bin
	go build -o bin/ ./cmd/...

test: ## Run all tests with the race detector
	go test -race -count=1 ./...

test-short: ## Skip the slow integration tests
	go test -short ./...

cover: ## Open a test coverage report in the browser
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint: ## Static analysis
	go vet ./...
	golangci-lint run

run-sim: ## Start the device simulator
	go run ./cmd/simulator

run-gateway: ## Start the gateway with the local config
	go run ./cmd/gateway -config configs/gateway.local.yaml

run-ingestor: ## Start the ingestor against local services
	go run ./cmd/ingestor

up: ## Start the whole stack with Docker
	docker compose up -d --build

down: ## Stop the stack (data volumes are kept)
	docker compose down

logs: ## Follow the logs of every service
	docker compose logs -f