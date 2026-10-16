SHELL := /usr/bin/env bash

# Project part of the Makefile; shared targets (ci, build, test, lint, vuln, coverage-check, image, ...)
# come from make/common.mk. Coverage thresholds (80% total, 60% per package) are the shared defaults;
# the ratchet history is in docs/coverage.md.

APP             := bridge-kafka-service
CMD             := ./cmd/bridge-kafka-service
FMT_DIRS        := cmd internal testutil
COVERAGE_SCRIPT := scripts/coverage.sh
TEST_DEPS       := test-scripts
CI_EXTRA        := proto-check docs-check

include make/common.mk

PROTO_DIR := internal/schemas/model
PROTO_FILES := $(PROTO_DIR)/service.proto
PROTO_OUT := internal/transport/modelrpc
# Plugins are pinned as `tool` dependencies in go.mod; protoc itself must be installed.
PROTOC_FLAGS = -I $(PROTO_DIR) \
	--plugin=protoc-gen-go=$(shell go tool -n protoc-gen-go) \
	--plugin=protoc-gen-go-grpc=$(shell go tool -n protoc-gen-go-grpc) \
	--go_opt=module=bridge-kafka-service --go-grpc_opt=module=bridge-kafka-service

.PHONY: check proto proto-check test-scripts docs-check

# Tests of the shared docs-check script.
test-scripts:
	sh scripts/test_docs_check.sh

# Every config key (yaml and mapstructure tags) is mentioned in the README and the app-configuration spec.
docs-check:
	CONFIG_SRC="internal/config/config.go internal/schemas/kafka.go" \
	DOCS="README.md openspec/specs/app-configuration/spec.md" sh scripts/docs_check.sh

# Kept for compatibility: the former aggregate target; same checks as `ci` plus a build.
check: build ci

# Regenerate the committed gRPC stubs from the .proto sources.
proto:
	mkdir -p $(PROTO_OUT)
	protoc $(PROTOC_FLAGS) --go_out=. --go-grpc_out=. $(PROTO_FILES)

# Fail when the committed stubs differ from a fresh generation (protoc's own
# version header is ignored so a different local protoc does not cause noise).
# Without protoc the check is skipped locally and is an error when CI is set.
proto-check:
	@if ! command -v protoc >/dev/null; then \
		if [ -n "$$CI" ]; then echo "proto-check: protoc is required in CI" >&2; exit 1; fi; \
		echo "proto-check skipped: protoc is not installed"; exit 0; \
	fi; \
	tmp=$$(mktemp -d) && trap 'rm -rf $$tmp' EXIT && \
	protoc $(PROTOC_FLAGS) --go_out=$$tmp --go-grpc_out=$$tmp $(PROTO_FILES) && \
	if diff -r -x '*_test.go' -I 'protoc  *v[0-9]' $(PROTO_OUT) $$tmp/$(PROTO_OUT); then \
		echo "proto: generated code is up to date"; \
	else \
		echo "proto: generated code is stale, run 'make proto'" >&2; exit 1; \
	fi
