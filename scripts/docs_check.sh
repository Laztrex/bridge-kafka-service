#!/bin/sh
# Checks docs against code: every config key (a yaml:"..." or mapstructure:"..." struct tag in
# CONFIG_SRC) must be documented in each of DOCS as "key:", as in the YAML examples.
# Shared by the Go services (docs/conventions.md); keep the copies identical.
#
# CONFIG_SRC and DOCS are space-separated lists; the script tests override them.

CONFIG_SRC="${CONFIG_SRC:-internal/config/config.go}"
DOCS="${DOCS:-README.md openspec/specs/configs/spec.md}"

# shellcheck disable=SC2086 # CONFIG_SRC is a list of files
keys=$(grep -ho -E '(yaml|mapstructure):"[^",]*' $CONFIG_SRC | sed -E 's/^(yaml|mapstructure):"//' | grep -v -e '^-$' -e '^$' | sort -u)
if [ -z "$keys" ]; then
    echo "ERROR: no config keys found in $CONFIG_SRC" >&2
    exit 1
fi

missing=0
for doc in $DOCS; do
    for key in $keys; do
        if ! grep -Eq "(^|[^A-Za-z0-9_])${key}:" "$doc"; then
            echo "undocumented config key: $key ($doc)" >&2
            missing=1
        fi
    done
done

if [ "$missing" -ne 0 ]; then
    echo "ERROR: documentation lags behind the config code (see above)" >&2
    exit 1
fi
echo "docs-check: all config keys documented ($(echo "$keys" | wc -l | tr -d ' ') keys)"
