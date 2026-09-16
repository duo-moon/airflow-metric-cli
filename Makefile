BINARY      := afmetric
PKG         := github.com/duo-moon/airflow-metric-cli
CMD_PKG     := $(PKG)/cmd/afmetric
BIN_DIR     := bin
VERSION_PKG := $(PKG)/internal/version

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X $(VERSION_PKG).Version=$(VERSION) \
	-X $(VERSION_PKG).Commit=$(COMMIT) \
	-X $(VERSION_PKG).Date=$(DATE)

.PHONY: all build test lint vet tidy run clean generate docker-build \
	demo-up demo-down demo-ping demo-run \
	demo-up-v3 demo-down-v3 demo-ping-v3 demo-run-v3 demo-token-v3

all: build

build:
	@mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) $(CMD_PKG)

test:
	CGO_ENABLED=1 go test -race -count=1 ./...

vet:
	go vet ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found; falling back to go vet"; \
		go vet ./...; exit 0; }
	golangci-lint run ./...

tidy:
	go mod tidy

run: build
	./$(BIN_DIR)/$(BINARY) $(ARGS)

clean:
	rm -rf $(BIN_DIR)

generate:
	go generate ./...

DOCKER_IMAGE ?= afmetric:$(VERSION)

docker-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg DATE=$(DATE) \
		-t $(DOCKER_IMAGE) \
		.

# --- Local Airflow demo (docker-compose) --------------------------------
# Boots a small Airflow stack (postgres + init + web/api-server + scheduler
# + [dag-processor v3 only] + triggerer) with a fixed admin/admin login so
# afmetric can be pointed at it without a real cluster. Not for production.
#
# Two flavours behind docker-compose profiles:
#   v2: apache/airflow:2.11.2, /api/v1, basic auth
#   v3: apache/airflow:3.2.0,  /api/v2, JWT (fetched via /auth/token)
# postgres is shared. Only one profile should be up at a time — schema
# is not compatible across generations.

DEMO_COMPOSE      := docker compose -f demo/docker-compose.yaml
DEMO_COMPOSE_V2   := $(DEMO_COMPOSE) --profile v2
DEMO_COMPOSE_V3   := $(DEMO_COMPOSE) --profile v3
DEMO_AIRFLOW_ENV  := AIRFLOW_URL=http://localhost:8080 AIRFLOW_USERNAME=admin AIRFLOW_PASSWORD=admin

demo-up:
	$(DEMO_COMPOSE_V2) up -d
	@echo "waiting for Airflow 2.x to become healthy (up to ~3 min on first run)…"
	@for i in $$(seq 1 36); do \
		status=$$($(DEMO_COMPOSE_V2) ps --format '{{.Health}}' airflow-webserver 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then echo "airflow is healthy — login admin/admin at http://localhost:8080"; exit 0; fi; \
		sleep 5; \
	done; \
	echo "timeout — check '$(DEMO_COMPOSE_V2) logs airflow-webserver'"; exit 1

demo-ping: build
	$(DEMO_AIRFLOW_ENV) ./$(BIN_DIR)/$(BINARY) ping

demo-run: build
	$(DEMO_AIRFLOW_ENV) ./$(BIN_DIR)/$(BINARY) run

demo-down:
	$(DEMO_COMPOSE_V2) down -v

# --- v3 flavour ---------------------------------------------------------
# JWT is the only auth for /api/v2. `demo-token-v3` hits /auth/token with
# admin/admin and prints the access_token; `demo-ping-v3` / `demo-run-v3`
# call it inline so the token stays out of the shell history.

demo-up-v3:
	$(DEMO_COMPOSE_V3) up -d
	@echo "waiting for Airflow 3.x api-server to become healthy (up to ~3 min on first run)…"
	@for i in $$(seq 1 36); do \
		status=$$($(DEMO_COMPOSE_V3) ps --format '{{.Health}}' airflow-apiserver 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then echo "airflow is healthy — login admin/admin at http://localhost:8080"; exit 0; fi; \
		sleep 5; \
	done; \
	echo "timeout — check '$(DEMO_COMPOSE_V3) logs airflow-apiserver'"; exit 1

demo-token-v3:
	@curl -sf -X POST http://localhost:8080/auth/token \
		-H "Content-Type: application/json" \
		-d '{"username":"admin","password":"admin"}' \
	| sed -E 's/.*"access_token"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/'

demo-ping-v3: build
	@AIRFLOW_URL=http://localhost:8080 \
	 AIRFLOW_TOKEN=$$($(MAKE) --no-print-directory demo-token-v3) \
	 ./$(BIN_DIR)/$(BINARY) --api-version v2 ping

demo-run-v3: build
	@AIRFLOW_URL=http://localhost:8080 \
	 AIRFLOW_TOKEN=$$($(MAKE) --no-print-directory demo-token-v3) \
	 ./$(BIN_DIR)/$(BINARY) --api-version v2 run

demo-down-v3:
	$(DEMO_COMPOSE_V3) down -v
