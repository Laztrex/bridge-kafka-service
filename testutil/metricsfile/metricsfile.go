// Package metricsfile reads the JSON-lines metrics file written by the
// transaction logger, so tests can assert on recorded metric values.
package metricsfile

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"bridge-kafka-service/internal/consts"
)

// Durations returns request id -> duration_ms recorded for eventType.
func Durations(t testing.TB, file string, eventType consts.EventType) map[string]int64 {
	t.Helper()
	f, err := os.Open(file)
	if err != nil {
		t.Fatalf("metricsfile: open: %v", err)
	}
	defer f.Close()

	out := map[string]int64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line map[string]string
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("metricsfile: bad line %q: %v", sc.Text(), err)
		}
		if line["metric_name"] != consts.MetricsDurationMapName[eventType] {
			continue
		}
		v, err := strconv.ParseInt(line["metric_value"], 10, 64)
		if err != nil {
			t.Fatalf("metricsfile: bad value %q: %v", line["metric_value"], err)
		}
		out[line[consts.FieldMetricRquid]] = v
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("metricsfile: scan: %v", err)
	}
	return out
}
