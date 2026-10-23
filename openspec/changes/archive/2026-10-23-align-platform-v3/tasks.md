# Tasks

## 1. Toolchain alignment

- [x] 1.1 Verify the registered platform store is clean at v3.0.0, review its v2-to-v3 spec diff and confirm openspec doctor resolves platform
- [x] 1.2 Raise go.mod and the default Docker builder to 1.27.3, update README and OpenSpec context, and verify go build ./..., go vet ./... and go test -race ./...

## 2. Alignment verification

- [x] 2.1 Validate this change with openspec validate align-platform-v3 --strict and refresh graphify; after successful checks, record aligned_with: v3.0.0
- [x] 2.2 Run make ci
