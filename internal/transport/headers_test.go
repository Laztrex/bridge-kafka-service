package transport_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/mockmodel"
	"bridge-kafka-service/testutil/requiredheaders"
)

func TestHTTPHeaderIssues(t *testing.T) {
	up := transport.NewHTTPUpstream(&schemas.ConsumerConfig{Service: schemas.HostConfig{Name: "svc", Port: "1"}})
	cases := []struct {
		name, key, value string
		issue            bool
	}{
		{"line feed", "note", "a\nb", true},
		{"carriage return", "note", "a\rb", true},
		{"vertical tab", "note", "a\vb", true},
		{"form feed", "note", "a\fb", true},
		{"tab is valid", "note", "a\tb", false},
		{"non-ASCII text is valid", "note", "héllo", false},
		{"invalid name", "bad name", "v", true},
		{"transport-controlled names are left to the call", "Content-Length", "a\nb", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := transport.HeaderIssues(up, map[string]string{c.key: c.value})
			if !c.issue {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, []transport.HeaderIssue{{Name: c.key, Drop: true}}, got)
		})
	}
}

func TestGRPCHeaderIssues(t *testing.T) {
	up := grpcUpstream(t, grpcConf(t, "127.0.0.1:1", schemas.ServiceTLS{}))

	assert.Equal(t, []transport.HeaderIssue{{Name: "note", Drop: true}},
		transport.HeaderIssues(up, map[string]string{"note": "\xff\xfe"}), "invalid UTF-8 cannot go into details")
	assert.Equal(t, []transport.HeaderIssue{{Name: consts.HeaderRequestIdKey, Drop: false}},
		transport.HeaderIssues(up, map[string]string{consts.HeaderRequestIdKey: "ïd"}), "only the metadata copy is skipped")
	assert.Empty(t, transport.HeaderIssues(up, map[string]string{"note": "a\nb", "Content-Type": "text/plain"}),
		"gRPC carries what HTTP cannot")
}

func TestGRPC_InvalidUTF8HeaderIsDroppedAndTheCallReachesTheService(t *testing.T) {
	srv := mockmodel.Start(t)
	srv.Respond([]byte(`{"ok":true}`))
	up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))
	logs := captureLog(t)
	ev, _ := newEvent("resp")
	ev.Values.Body.Headers["note"] = "\xff\xfe"
	q := transport.NewResponseQueue()

	transport.SendToService(context.Background(), logger(t), up, ev, q, 5*time.Second)

	require.Len(t, srv.Requests(), 1, "the call reaches the service")
	assert.NotContains(t, srv.Requests()[0].Details, "note")
	require.Len(t, q, 1)
	out := <-q
	assert.Empty(t, out.Values.Body.Headers["Grpc-Status"], "no client-side Internal answer is published")
	assert.Equal(t, 1, logs.count("header cannot be carried by the upstream transport"))
	assert.Equal(t, "\xff\xfe", ev.Values.Body.Headers["note"], "the record keeps the header for audit")
}

func TestHTTP_TransportControlledHeadersAreNotForwarded(t *testing.T) {
	var got http.Header
	var contentLength int64
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, contentLength = r.Header.Clone(), r.ContentLength
	}))
	t.Cleanup(srv.Close)
	logs := captureLog(t)
	ev, _ := newEvent("")
	for k, v := range map[string]string{
		"Content-Type": "text/plain", "Content-Length": "999", "Connection": "Upgrade", "Keep-Alive": "timeout=5", "Upgrade": "h2c",
	} {
		ev.Values.Body.Headers[k] = v
	}

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "")), ev, nil, time.Second)

	require.NotNil(t, got)
	assert.Equal(t, "application/json", got.Get("Content-Type"))
	assert.Equal(t, int64(len(`{"q":1}`)), contentLength, "the call's own framing is used")
	assert.Empty(t, got.Get("Keep-Alive"))
	assert.Empty(t, got.Get("Upgrade"))
	assert.Equal(t, "v", got.Get("X-Custom"), "other headers are still forwarded")
	assert.Zero(t, logs.count("header cannot be carried"), "transport-controlled headers are left out without a warning")
}

func TestRequiredHeaders_NeverTransportControlled(t *testing.T) {
	candidates := append([]string{consts.HeaderRequestIdKey, consts.HeaderRequestTimeKey, consts.HeaderTraceIdKey, consts.HeaderClientId},
		consts.RequiredHeaders...)
	for _, name := range candidates {
		assert.False(t, transport.IsTransportControlled(name), "%s would be both required and dropped by HTTP", name)
	}
}

// recordingUpstream answers every call and keeps the headers it received.
type recordingUpstream struct {
	calls atomic.Int32
	got   map[string]string
}

func (r *recordingUpstream) Call(_ context.Context, req transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	r.calls.Add(1)
	r.got = req.Headers
	return transport.UpstreamResponse{Body: []byte(`{}`)}, nil
}

func TestHeaderPolicy_OptionalHeaderHTTPCannotCarryIsDropped(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	t.Cleanup(srv.Close)
	logs := captureLog(t)
	ev, acks := newEvent("")
	ev.Values.Body.Headers["note"] = "a\nb"

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "")), ev, nil, time.Second)

	require.NotNil(t, got, "the call is made")
	assert.Empty(t, got.Get("Note"))
	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 1, *acks)
	assert.Equal(t, 1, logs.count(`"header":"note"`), "the warning names the header")
	assert.Equal(t, 1, logs.count(`"x-request-id":"req-1","header":"note"`), "and the request id")
	assert.Equal(t, "a\nb", ev.Values.Body.Headers["note"], "the record keeps the header")
}

func TestHeaderPolicy_OptionalRequestIdInvalidKeepsTheRecordsIdOnTheResponse(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	t.Cleanup(srv.Close)
	ev, _ := newEvent("resp")
	ev.Values.RequestId, ev.Values.Body.Headers[consts.HeaderRequestIdKey] = "a\nb", "a\nb"
	q := transport.NewResponseQueue()

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, q, time.Second)

	require.NotNil(t, got)
	assert.NotContains(t, got.Get(consts.HeaderRequestIdKey), "\n", "the record's id is left out; the client stamps a fresh one")
	assert.NotEmpty(t, got.Get(consts.HeaderRequestIdKey))
	require.Len(t, q, 1)
	assert.Equal(t, "a\nb", (<-q).Values.Body.Headers[consts.HeaderRequestIdKey], "the response record carries the record's id")
}

func TestHeaderPolicy_RequiredHeaderAbsentIsNotSent(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderTraceIdKey)
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	logs := captureLog(t)
	up := &recordingUpstream{}
	ev, acks := newEvent("")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, nil, time.Second, transport.NewRetryState())

	assert.Zero(t, up.calls.Load(), "the upstream is not called")
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks, "settled without a retry")
	assert.Equal(t, 1, logs.count("required header "+consts.HeaderTraceIdKey+" is missing"))
}

func TestHeaderPolicy_RequiredHeaderInvalidForTheTransport(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderRequestIdKey)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	t.Cleanup(srv.Close)
	ev, acks := newEvent("")
	ev.Values.RequestId, ev.Values.Body.Headers[consts.HeaderRequestIdKey] = "a\rb", "a\rb"

	transport.SendToServiceRetrying(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "")), ev, nil, time.Second, transport.NewRetryState())

	assert.Zero(t, calls.Load())
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks)
	assert.Equal(t, "a\rb", ev.Event.RequestId, "logs, metrics and audit keep the record's id")
}

func TestHeaderPolicy_RequiredHeaderMatchesIgnoringCase(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderClientId)
	up := &recordingUpstream{}
	ev, _ := newEvent("")
	delete(ev.Values.Body.Headers, consts.HeaderClientId)
	ev.Values.Body.Headers["X-Client-Id"] = "billing"

	transport.SendToService(context.Background(), logger(t), up, ev, nil, time.Second)

	assert.Equal(t, int32(1), up.calls.Load())
	assert.Equal(t, 1, ev.Event.Success)
}

func TestHeaderPolicy_UpstreamWithoutCheckerGetsHeadersUnchanged(t *testing.T) {
	up := &recordingUpstream{}
	ev, _ := newEvent("")
	ev.Values.Body.Headers["note"] = "a\nb"

	transport.SendToService(context.Background(), logger(t), up, ev, nil, time.Second)

	assert.Equal(t, "a\nb", up.got["note"])
}
