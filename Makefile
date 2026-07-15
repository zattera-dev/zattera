SHELL := /bin/bash
BIN := $(CURDIR)/bin
export CGO_ENABLED := 0

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/zattera-dev/zattera/internal/pkgutil/version.Version=$(VERSION)

# Pinned tool versions (installed into ./bin by `make tools`)
BUF_VERSION := v1.50.0
PROTOC_GEN_GO_VERSION := v1.36.5
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
GRPC_GATEWAY_VERSION := v2.26.1
GOLANGCI_LINT_VERSION := v2.6.2

.PHONY: all
all: build

.PHONY: tools
tools: ## Install pinned build tools into ./bin
	GOBIN=$(BIN) go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	GOBIN=$(BIN) go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@$(GRPC_GATEWAY_VERSION)
	GOBIN=$(BIN) go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@$(GRPC_GATEWAY_VERSION)
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: generate
generate: ## Regenerate protobuf/gRPC/gateway code (commit the result)
	$(BIN)/buf generate
	$(BIN)/buf generate --template buf.gen.openapi.yaml --path api/proto/zattera/v1

.PHONY: proto-lint
proto-lint:
	$(BIN)/buf lint

.PHONY: build
build: ## Full binary (CLI + server) for the host platform
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/zattera ./cmd/zattera

.PHONY: build-cli
build-cli: ## CLI-only binary
	go build -trimpath -tags cli_only -ldflags '$(LDFLAGS)' -o $(BIN)/zattera-cli ./cmd/zattera

.PHONY: build-server
build-server: ## Server-only binary
	go build -trimpath -tags server_only -ldflags '$(LDFLAGS)' -o $(BIN)/zatterad ./cmd/zattera

.PHONY: cross
cross: ## Cross-compile release matrix into ./dist
	mkdir -p dist
	GOOS=linux  GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/zattera-linux-amd64 ./cmd/zattera
	GOOS=linux  GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/zattera-linux-arm64 ./cmd/zattera
	GOOS=darwin GOARCH=amd64 go build -trimpath -tags cli_only -ldflags '$(LDFLAGS)' -o dist/zattera-darwin-amd64 ./cmd/zattera
	GOOS=darwin GOARCH=arm64 go build -trimpath -tags cli_only -ldflags '$(LDFLAGS)' -o dist/zattera-darwin-arm64 ./cmd/zattera
	GOOS=windows GOARCH=amd64 go build -trimpath -tags cli_only -ldflags '$(LDFLAGS)' -o dist/zattera-windows-amd64.exe ./cmd/zattera

.PHONY: lint
lint: proto-lint ## Lint Go + protos
	$(BIN)/golangci-lint run

.PHONY: test
test: ## Unit tests (no Docker; includes simcluster)
	CGO_ENABLED=1 go test -race ./...

.PHONY: test-integration
test-integration: ## Integration tests (require a running Docker daemon)
	CGO_ENABLED=1 go test -race -tags integration -count=1 -timeout 20m ./test/integration/...

.PHONY: test-e2e
test-e2e: build ## Full single-node E2E smoke test (requires Docker)
	go test -tags e2e -count=1 -timeout 30m ./test/e2e/...

.PHONY: test-chaos
test-chaos: ## Simulated-cluster chaos tests (no Docker, slow)
	go test -tags chaos -count=1 -timeout 30m ./test/chaos/...

.PHONY: test-cloud
test-cloud: ## Real-cloud cluster tests (needs HCLOUD_TOKEN; SPINS PAID VMs)
	go test -tags cloud -count=1 -timeout 30m -v ./test/cloud/...

.PHONY: cloud-reap
cloud-reap: ## Destroy ALL leftover harness cloud resources (needs HCLOUD_TOKEN)
	go test -tags cloud -count=1 -run TestCloudReap -v ./test/cloud/...

.PHONY: check-generate
check-generate: generate ## CI: fail if generated code is stale
	git diff --exit-code -- api/gen api/openapi

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'
