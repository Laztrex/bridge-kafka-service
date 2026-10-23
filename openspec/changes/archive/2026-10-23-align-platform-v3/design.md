# Design

## Context

See proposal.md for scope. The tagged v2.0.0 to v3.0.0 spec diff changes only GOQ-1. The registered store is /Users/laztrex/openspec/platform; the sibling authoring repository has unrelated local edits and is not used as release content. The local effective toolchain is go1.27.3 darwin/arm64.

## Decisions

- Choose the minimum permitted patch, 1.27.3, for go.mod and the official builder image. A later permitted patch adds no benefit required by this alignment.
- Keep CI's existing go-version-file selection so build and test versions follow go.mod.
- Keep historical archived artifacts unchanged. Update the current README and context; clarify the existing wire identifiers from project-identity.
- Do not copy GOQ-1 into service specs. No runtime behavior changes, so skip spec deltas.

## Risks / Trade-offs

- An overridden builder can be older than the module minimum: document the platform's stable Go 1.27.x constraint.
- Image construction needs Docker and registry access: report separately if it cannot be verified locally.

## Migration Plan

Update module, default image and docs together. Verify build, vet, race tests and make ci with Go 1.27.3; validate the OpenSpec change and refresh graphify. Record v3.0.0 alignment only after successful checks. Rollback requires restoring the former module, builder and alignment marker together.

## Validation Results

Validated on 2026-10-23 against the clean registered v3.0.0 store. Build, vet and race tests passed with go1.27.3 darwin/arm64. `make ci` passed: lint reported zero issues, govulncheck found no reachable vulnerabilities, coverage was 94.4% with every included package above 60%, and proto-check and docs-check passed. `make image BUILD_TAGS=release` built successfully using golang:1.27.3. Strict OpenSpec validation passed and `graphify update .` completed. Local TCP tests and Docker required execution outside the filesystem sandbox.
