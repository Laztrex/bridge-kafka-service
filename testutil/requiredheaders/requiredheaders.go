// Package requiredheaders lets tests fill consts.RequiredHeaders for one test.
package requiredheaders

import (
	"testing"

	"bridge-kafka-service/internal/consts"
)

// Set replaces the required-headers list with names for the duration of t. Tests that call it must
// not run in parallel.
func Set(t testing.TB, names ...string) {
	t.Helper()
	old := consts.RequiredHeaders
	consts.RequiredHeaders = names
	t.Cleanup(func() { consts.RequiredHeaders = old })
}
