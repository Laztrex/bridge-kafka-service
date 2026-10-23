# Proposal

## Why

The service records platform v2.0.0, but the published v3.0.0 raises GOQ-1 to a minimum stable Go 1.27.3 toolchain. Its module and default builder still use 1.27.2. SDD-2 requires alignment before feature work.

## What Changes

- Use the clean registered platform store at release v3.0.0 (SDD-4, SDD-8).
- Raise the module minimum and default builder to Go 1.27.3 (GOQ-1, ENV-1).
- Update current toolchain documentation and OpenSpec context.
- Record alignment with v3.0.0 (SDD-6) after verifying the toolchain and platform checks (GOQ-3 through GOQ-6).

## Capabilities

No new or modified service capabilities. This tooling change uses `skip_specs: true`; GOQ-1 remains defined only in the platform store (SDD-9). Existing service specs, including quality-gates and project-identity, require no deltas.

## Non-goals

No new service behavior, dependency upgrades, exemptions, platform-store declaration in the service, or changes to the earlier v2 alignment work.

## Impact

Affected files: go.mod, Dockerfile, README.md, openspec/config.yaml and .platform-standards.yaml. CI already selects its Go version from go.mod. Existing consumers and runtime configuration retain their contracts.
