# Tasks

## 1. Consolidation

- [x] 1.1 Write the decision log in `design.md`, keeping every decision that still holds and recording reversed ones as rejected alternatives. Verify by checking each consolidated record's design against the log
- [x] 1.2 Point code comments that cite consolidated records at the baseline IDs. Verify with `grep -rn "design D[0-9]" internal cmd testutil` returning nothing
- [x] 1.3 Replace the consolidated records in `openspec/changes/archive/` with this baseline. Verify that `openspec/changes/archive/` holds only this baseline

## 2. Integration

- [x] 2.1 Run `make ci` and fix any failures
