# CI Coverage Gate

The gate runs in CI as part of `make ci` (the `Checks` step of `.github/workflows/ci.yml`). The `.gitlab-ci.yml` fragment below is kept for reference, for other CI runners.

## Why the gate

- The threshold (`COVER_THRESHOLD`) and the per-package floor (`PACKAGE_FLOOR`) are the same for local development and CI; the long-term target is 80% total and 60% per package.
- The threshold ladder (see below) applies upward pressure; the bar is raised by changing one variable (`COVER_THRESHOLD`/`PACKAGE_FLOOR`, defaults in `make/common.mk`), without code changes.
- `scripts/coverage.sh` excludes `*.pb.go`, `testutil/`, `test/`, `cmd/` and `examples/` so generated code, the thin entrypoint and runnable examples do not skew the percentage.

## Recommended `.gitlab-ci.yml` fragment

```yaml
coverage-check:
  stage: test
  image: golang:1.27.2
  variables:
    COVER_THRESHOLD: "35"        # current ladder step, see table below
  script:
    - make coverage-check
  artifacts:
    when: always
    paths:
      - coverage.out
      - coverage.html
    expire_in: 7 days
```

## Script behavior

`scripts/coverage.sh <profile> [threshold] [floor]` computes **statement-weighted** coverage (like `go tool cover`) and checks two conditions:

1. total coverage `>= COVER_THRESHOLD` (default **80**, set in `make/common.mk`);
2. coverage of **every** package `>= PACKAGE_FLOOR` (default **60**), so a well-covered package cannot mask an uncovered one.

Exactly these groups are excluded: generated protobuf (`*.pb.go`), the `testutil/` and `test/` helper packages and the thin `cmd/` entrypoint (flag parsing and `signal.NotifyContext` → `app.Connect`; `Connect` itself is covered by tests). The exclusion list must not grow without a separate change.

| Exit code | Meaning |
|---|---|
| `0` | `OK total <pct>% >= threshold ...`: both conditions hold. |
| `1` | `FAIL total ...` and/or `FAIL package <pkg> ...`: the threshold or floor is not met; the stage must fail. |
| `2` | `ERROR: profile ... not found.`: run `make coverage` first. |
| `3` | The profile is empty after filtering exclusions: needs manual investigation. |

## Threshold ladder

The threshold grew in steps within one change (`raise-coverage-to-80-and-prune-specs`); the final step is reached and is the default:

| Step | `COVER_THRESHOLD` | `PACKAGE_FLOOR` | Status |
|---|---|---|---|
| Initial | `35` | — | passed |
| After `router` | `60` | `0` | passed |
| After `observability` and `utils` | `70` | `0` | passed |
| Final | `80` | `60` | **in effect** (actual: 94.1% total, lowest package 91.0%) |

Override for a local check: `COVER_THRESHOLD=50 PACKAGE_FLOOR=40 make coverage-check`.

## Generated code check

`make proto-check` regenerates the gRPC stubs into a temp directory and compares them with the committed ones (`internal/transport/modelrpc`); the `protoc` version line is ignored. In CI (the `CI` variable is set) a missing `protoc` is an error. Add it to the pipeline after `make coverage-check`:

```yaml
proto-check:
  stage: test
  image: golang:1.27.2   # + protoc installed
  script:
    - make proto-check
```
