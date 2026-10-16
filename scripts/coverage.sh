#!/usr/bin/env bash
# Coverage gate: total statement coverage must reach THRESHOLD and every
# package must reach FLOOR. Excluded from the calculation: generated protobuf
# code (*.pb.go), test helpers (testutil/, test/), the thin cmd/ entrypoint
# and runnable examples (examples/).
# Shared by Go services (docs/conventions.md); keep the copies identical.
set -euo pipefail

PROFILE="${1:-coverage.out}"
THRESHOLD="${2:-${COVER_THRESHOLD:-80}}"
FLOOR="${3:-${PACKAGE_FLOOR:-60}}"

if [[ ! -f "$PROFILE" ]]; then
  echo "ERROR: profile '$PROFILE' not found. Run 'make coverage' first." >&2
  exit 2
fi

# Profile lines: <file>:<start>,<end> <statements> <count>
REPORT=$(awk '
  /^mode:/ { next }
  $1 ~ /\.pb\.go:/ { next }
  $1 ~ /\/testutil\// { next }
  $1 ~ /\/test\// { next }
  $1 ~ /\/cmd\// { next }
  $1 ~ /\/examples\// { next }
  {
    file = $1
    sub(/:[0-9]+\.[0-9]+,[0-9]+\.[0-9]+$/, "", file)
    pkg = file
    sub(/\/[^\/]+$/, "", pkg)
    total[pkg] += $2
    all += $2
    if ($3 != "0") { hit[pkg] += $2; allhit += $2 }
  }
  END {
    if (all == 0) exit 3
    for (p in total) printf "PKG %s %.1f\n", p, hit[p] / total[p] * 100
    printf "TOTAL %.1f\n", allhit / all * 100
  }
' "$PROFILE") || {
  rc=$?
  (( rc == 3 )) && echo "ERROR: no coverage data after exclusion filter" >&2
  exit $rc
}

echo "Per-package coverage (floor ${FLOOR}%):"
echo "$REPORT" | LC_ALL=C sort | awk '$1 == "PKG" { printf "  %-60s %5.1f%%\n", $2, $3 }'

PCT=$(echo "$REPORT" | awk '$1 == "TOTAL" { print $2 }')
status=0

while read -r _ pkg pct; do
  if awk -v p="$pct" -v f="$FLOOR" 'BEGIN { exit !(p < f) }'; then
    printf "FAIL package %s at %.1f%% < floor (%s%%)\n" "$pkg" "$pct" "$FLOOR" >&2
    status=1
  fi
done < <(echo "$REPORT" | awk '$1 == "PKG"')

if awk -v p="$PCT" -v t="$THRESHOLD" 'BEGIN { exit !(p < t) }'; then
  printf "FAIL total %.1f%% < threshold (%s%%)\n" "$PCT" "$THRESHOLD" >&2
  status=1
fi

(( status == 0 )) && printf "OK total %.1f%% >= threshold (%s%%), all packages >= %s%%\n" "$PCT" "$THRESHOLD" "$FLOOR"
exit $status
