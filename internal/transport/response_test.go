package transport_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// TestPublishUpstreamResponse_QueuedEventCarriesTheStanzaConfig verifies that the
// response event handed to the publish channel keeps the consumer stanza config,
// so the response publisher can apply its failure policy.
func TestPublishUpstreamResponse_QueuedEventCarriesTheStanzaConfig(t *testing.T) {
	q := transport.NewResponseQueue()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	conf := upstreamConf(t, srv, "resp")
	conf.OnFailure = schemas.FailureRetry
	conf.Service.MaxConn = 3
	ev, _ := newEvent("resp")
	ev.Config = conf

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(conf), ev, q, 3*time.Second)

	select {
	case out := <-q:
		require.NotNil(t, out.Config, "the queued event carries the stanza config")
		assert.Same(t, conf, out.Config)
		assert.Equal(t, schemas.FailureRetry, out.Config.EffectiveFailurePolicy())
		out.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("response event not published")
	}
}

// A response dropped from a full queue under on_failure: skip is recorded exactly once as a failed
// response_service publication, with its egress audit entry, and the record is settled.
func TestPublishUpstreamResponse_DroppedResponseIsRecordedAsFailure(t *testing.T) {
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.Run()
	l.RunAudit()
	q := transport.NewResponseQueue()
	fillQueue(q)
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), l, &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, q, time.Second)
	l.Shutdown()

	assert.Equal(t, 1, *acks, "the record is still settled")
	assert.Equal(t, 1, ev.Event.Success, "the upstream call itself succeeded")
	assert.Equal(t, 1, metricLines(t, file, consts.MetricsFailureMapName[consts.EventResponseService], "1", "req-1"),
		"one response_service failure for the dropped response")
	assert.Zero(t, metricLines(t, file, consts.MetricsSuccessMapName[consts.EventResponseService], "1", "req-1"))
	audit, err := os.ReadFile(file + ".audit")
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(string(audit), "envoy-egress-als"), "one egress audit entry")
}

// metricLines counts metric lines with the given name, value and request id.
func metricLines(t *testing.T, file, name, value, requestId string) int {
	t.Helper()
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var fields map[string]string
		require.NoError(t, json.Unmarshal([]byte(line), &fields))
		if fields["metric_name"] == name && fields["metric_value"] == value && fields[consts.FieldMetricRquid] == requestId {
			n++
		}
	}
	return n
}
