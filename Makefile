# Go Kafka Pipeline Makefile

.PHONY: help build run test clean docker-build docker-run docker-stop deps fmt lint vet

# Default target
help:
	@echo "Available targets:"
	@echo "  build        - Build the application"
	@echo "  run          - Run the application locally"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run with Docker Compose"
	@echo "  docker-stop  - Stop Docker Compose"
	@echo "  deps         - Download dependencies"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  vet          - Run go vet"

# Build the application
build:
	@echo "Building Go Kafka Pipeline..."
	go build -o bin/kafka-pipeline main.go

# Run the application locally
run:
	@echo "Running Go Kafka Pipeline..."
	go run main.go

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	go clean

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t go-kafka-pipeline .

# Run with Docker Compose
docker-run:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d

# Stop Docker Compose
docker-stop:
	@echo "Stopping Docker Compose services..."
	docker-compose down

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run linter (requires golangci-lint)
lint:
	@echo "Running linter..."
	golangci-lint run

# Run go vet
vet:
	@echo "Running go vet..."
	go vet ./...

# Development setup
dev-setup: deps
	@echo "Setting up development environment..."
	@echo "Installing golangci-lint..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Performance test
perf-test:
	@echo "Running performance test..."
	curl -X POST "http://localhost:8080/send-test-data-concurrently?count=100000"

# Check application health
health-check:
	@echo "Checking application health..."
	curl -s http://localhost:8080/health | jq .

# View metrics
metrics:
	@echo "Fetching application metrics..."
	curl -s http://localhost:8080/metrics

# View tracking status
status:
	@echo "Checking processing status..."
	curl -s http://localhost:8080/tracking-status | jq .

# Reset tracking
reset:
	@echo "Resetting tracking..."
	curl -X POST http://localhost:8080/reset-tracking

# Full development cycle
dev: clean deps fmt vet build test

# Production build
prod-build:
	@echo "Building for production..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-w -s' -o bin/kafka-pipeline main.go