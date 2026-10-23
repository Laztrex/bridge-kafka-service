# Shared Makefile part of the Go services (platform spec core/platform-build-interface).
# Keep this file identical across repositories. The including Makefile defines, before the include:
#   APP              binary and image name (= repository name)
#   CMD              main package, e.g. ./cmd/<name>
#   FMT_DIRS         directories checked by gofmt
#   COVERAGE_SCRIPT  path to the shared coverage gate script
#   TEST_DEPS        extra prerequisites of `test` (may be empty)
#   CI_EXTRA         project-specific checks appended to `ci` (may be empty)

# Tool versions are pinned: local runs and CI use the same ones.
GOLANGCI_LINT_VERSION ?= v2.14.0
GOVULNCHECK_VERSION   ?= v1.9.0

# Tools are installed into ./bin (not in git) so nothing depends on what the system has installed.
BIN           := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GOVULNCHECK   := $(BIN)/govulncheck-$(GOVULNCHECK_VERSION)

# Build metadata embedded into the binary (main.version, main.commit, main.date).
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

# Go build tags, comma-separated (empty: a dev build). Release builds set BUILD_TAGS=release, which
# excludes dev-only code (//go:build !release). The Dockerfile takes the same BUILD_TAGS argument,
# so CI can run plain docker build; make only passes it through for local runs.
BUILD_TAGS ?=

# Coverage gate: total and per-package thresholds, in percent.
COVER_THRESHOLD ?= 80
PACKAGE_FLOOR   ?= 60

# Build and runtime images (see Dockerfile); empty means the Dockerfile default.
BUILDER_IMAGE ?=
RUNTIME_IMAGE ?=
IMAGE         ?= $(APP):dev

.PHONY: ci build fmt-check vet test coverage coverage-html coverage-check lint vuln image tools

# Every required check, as in CI.
ci: fmt-check vet test lint vuln coverage-check $(CI_EXTRA)

build:
	go build -trimpath $(if $(BUILD_TAGS),-tags $(BUILD_TAGS)) -ldflags "$(LDFLAGS)" -o $(BIN)/$(APP) $(CMD)

# Formatting check: fails if gofmt would change any file.
fmt-check:
	@unformatted="$$(gofmt -l $(FMT_DIRS))"; \
	if [ -n "$$unformatted" ]; then echo "not formatted (gofmt -w):"; echo "$$unformatted"; exit 1; fi

vet:
	go vet ./...
	go vet -tags integration ./...

# Fast unit tests: no docker needed (integration tests build only with the integration tag).
test: $(TEST_DEPS)
	go test -race -count=1 ./...

coverage:
	go test -covermode=atomic -coverprofile=coverage.out ./...

coverage-html: coverage
	go tool cover -html=coverage.out -o coverage.html

coverage-check: coverage
	bash $(COVERAGE_SCRIPT) coverage.out $(COVER_THRESHOLD) $(PACKAGE_FLOOR)

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

# Vulnerabilities in dependencies reachable from the code, including the Go standard library.
vuln: $(GOVULNCHECK)
	$(GOVULNCHECK) ./...

# Image build. BUILDER_IMAGE and RUNTIME_IMAGE are passed only when set.
image:
	docker build $(if $(BUILDER_IMAGE),--build-arg BUILDER_IMAGE=$(BUILDER_IMAGE)) \
		$(if $(RUNTIME_IMAGE),--build-arg RUNTIME_IMAGE=$(RUNTIME_IMAGE)) \
		$(if $(BUILD_TAGS),--build-arg BUILD_TAGS=$(BUILD_TAGS)) \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(DATE) \
		-t $(IMAGE) .

tools: $(GOLANGCI_LINT) $(GOVULNCHECK)

$(GOLANGCI_LINT):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv $(BIN)/golangci-lint $@

$(GOVULNCHECK):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	mv $(BIN)/govulncheck $@
