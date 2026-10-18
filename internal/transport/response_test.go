package transport_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// TestPublishUpstreamResponse_QueuedEventCarriesTheStanzaConfig verifies that the
// response event handed to the publish channel keeps the consumer stanza config,
// so the response publisher can apply its failure policy.
func TestPublishUpstreamResponse_QueuedEventCarriesTheStanzaConfig(t *testing.T) {
	drainPublish()
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

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(conf), ev, 3*time.Second)

	select {
	case out := <-transport.PublishChannel:
		require.NotNil(t, out.Config, "the queued event carries the stanza config")
		assert.Same(t, conf, out.Config)
		assert.Equal(t, schemas.FailureRetry, out.Config.EffectiveFailurePolicy())
		out.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("response event not published")
	}
	drainPublish()
}
