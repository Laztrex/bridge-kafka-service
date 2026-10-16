#!/bin/sh
# Tests for docs_check.sh next to this file: `sh <dir>/test_docs_check.sh`. Code and docs are replaced by sandbox files.
set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CHECK_SH="$SCRIPT_DIR/docs_check.sh"
FAILED=0

new_sandbox() {
    SB=$(mktemp -d)
    cat > "$SB/config.go" <<'GO'
type C struct {
	Dir   string `yaml:"dir"`
	Limit *int   `yaml:"queue_size"`
	Skip  string `yaml:"-"`
}
GO
    printf 'watch:\n  dir: tmp\nlimits:\n  queue_size: 10\n' > "$SB/readme.md"
    printf -- '- `dir:` directory\n- `queue_size:` capacity\n' > "$SB/spec.md"
}

# the result goes to $OUT and $CODE
run_case() {
    OUT=$(CONFIG_SRC="$SB/config.go" DOCS="$SB/readme.md $SB/spec.md" sh "$CHECK_SH" 2>&1)
    CODE=$?
}

check() { # check <name> <condition met 0/1>
    if [ "$2" -eq 0 ]; then echo "ok   - $1"; else echo "FAIL - $1"; FAILED=1; fi
}
contains() { case "$OUT" in *"$1"*) return 0 ;; *) return 1 ;; esac; }

# 1. All keys documented: the check passes, yaml:"-" is not a key
new_sandbox
run_case
[ $CODE -eq 0 ]; check "all documented: exit 0" $?

# 2. A new undocumented key: the check fails and names the key and file
new_sandbox
printf '\tNew string `yaml:"shutdown_grace"`\n' >> "$SB/config.go"
run_case
[ $CODE -ne 0 ]; check "undocumented key: non-zero exit" $?
contains "shutdown_grace"; check "undocumented key: key named" $?
contains "readme.md"; check "undocumented key: file named" $?

# 3. A key documented in only one document: the check fails
new_sandbox
printf '\tNew string `yaml:"shutdown_grace"`\n' >> "$SB/config.go"
printf '  shutdown_grace: 5s\n' >> "$SB/readme.md"
run_case
[ $CODE -ne 0 ]; check "partially documented: non-zero exit" $?
contains "spec.md"; check "partially documented: missing file named" $?

# 4. A mention without a colon (in text) does not count as documentation; neither does another key's prefix
new_sandbox
printf '\tNew string `yaml:"size"`\n' >> "$SB/config.go"
printf 'size of the queue, queue_size: 1\n' >> "$SB/readme.md"; printf 'size\n' >> "$SB/spec.md"
run_case
[ $CODE -ne 0 ]; check "a mention without \"key:\" does not count" $?

exit $FAILED
