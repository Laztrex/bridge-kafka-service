package transport_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/mockmodel"
)

func TestNewUpstream_SelectsByProtocol(t *testing.T) {
	up, err := transport.NewUpstream(&schemas.ConsumerConfig{Service: schemas.HostConfig{Name: "svc", Port: "1"}})
	require.NoError(t, err, "no protocol means http")
	assert.NotNil(t, up)

	_, err = transport.NewUpstream(&schemas.ConsumerConfig{Service: schemas.HostConfig{Protocol: "soap"}})
	assert.ErrorContains(t, err, `unsupported upstream protocol "soap"`)
}

func TestConcurrencyLimit(t *testing.T) {
	assert.Equal(t, 512, transport.ConcurrencyLimit(&schemas.ConsumerConfig{}),
		"max_conn 0 means fast mode with the built-in ceiling")
	assert.Equal(t, 512, transport.ConcurrencyLimit(&schemas.ConsumerConfig{
		Service: schemas.HostConfig{MaxConn: -1},
	}), "a negative max_conn also selects fast mode")
	assert.Equal(t, 8, transport.ConcurrencyLimit(&schemas.ConsumerConfig{
		Service: schemas.HostConfig{MaxConn: 8},
	}), "a positive max_conn is the limit itself")
}

func TestPublisherCount(t *testing.T) {
	withMaxConn := func(n int) *schemas.ConsumerConfig {
		return &schemas.ConsumerConfig{Service: schemas.HostConfig{MaxConn: n}}
	}
	assert.Equal(t, 1, transport.PublisherCount(withMaxConn(1)), "a sequential pipeline keeps one publisher, and its order")
	assert.Equal(t, 4, transport.PublisherCount(withMaxConn(4)), "below the cap the pipeline limit is the count")
	assert.Equal(t, transport.MaxPublishersPerPipeline, transport.PublisherCount(withMaxConn(0)), "fast mode is capped")

	transport.FastModeLimit(t, 3)
	assert.Equal(t, 3, transport.PublisherCount(withMaxConn(0)), "a fast-mode ceiling below the cap is the count")
}

// outcome is everything an operator can observe about one processed record.
type outcome struct {
	published bool
	success   int
	failure   int
	acks      int
	requestID string
	clientID  string
	body      string
}

// observe runs one record through the shared path over the given upstream.
func observe(t *testing.T, up transport.Upstream, topic string, timeout time.Duration) outcome {
	t.Helper()
	q := transport.NewResponseQueue()
	ev, acks := newEvent(topic)

	transport.SendToService(context.Background(), logger(t), up, ev, q, timeout)

	out := outcome{success: ev.Event.Success, failure: ev.Event.Failure, acks: *acks}
	select {
	case rec := <-q:
		out.published = true
		out.requestID = rec.Values.Body.Headers[consts.HeaderRequestIdKey]
		out.clientID = rec.Values.Body.Headers[consts.HeaderClientId]
		out.body = string(rec.Values.Body.Data)
		rec.Ack()
		out.acks = *acks
	default:
	}
	return out
}

// backends builds the same upstream behaviour once over HTTP and once over gRPC.
type backends struct {
	http func(t *testing.T) transport.Upstream
	grpc func(t *testing.T) transport.Upstream
}

func httpWith(h http.HandlerFunc) func(*testing.T) transport.Upstream {
	return func(t *testing.T) transport.Upstream {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		return transport.NewHTTPUpstream(upstreamConf(t, srv, ""))
	}
}

func grpcWith(setup func(*mockmodel.Server)) func(*testing.T) transport.Upstream {
	return func(t *testing.T) transport.Upstream {
		srv := mockmodel.Start(t)
		setup(srv)
		return grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))
	}
}

// Outcomes match; only an application error's body differs by design (HTTP carries the error body,
// gRPC publishes Grpc-Status instead).
func TestParity_HTTPAndGRPCBehaveIdentically(t *testing.T) {
	deadAddr := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()
		return l.Addr().String()
	}

	scenarios := []struct {
		name     string
		topic    string
		timeout  time.Duration
		b        backends
		sameBody bool
	}{
		{
			name: "success with JSON body", topic: "resp", timeout: 3 * time.Second, sameBody: true,
			b: backends{
				http: httpWith(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"a":1}`)) }),
				grpc: grpcWith(func(s *mockmodel.Server) { s.Respond([]byte(`{"a":1}`)) }),
			},
		},
		{
			name: "non-JSON body is stored as a JSON string", topic: "resp", timeout: 3 * time.Second, sameBody: true,
			b: backends{
				http: httpWith(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("plain text")) }),
				grpc: grpcWith(func(s *mockmodel.Server) { s.Respond([]byte("plain text")) }),
			},
		},
		{
			name: "empty body", topic: "resp", timeout: 3 * time.Second, sameBody: true,
			b: backends{
				http: httpWith(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
				grpc: grpcWith(func(s *mockmodel.Server) { s.Respond(nil) }),
			},
		},
		{
			name: "no response topic settles right after the call", topic: "", timeout: 3 * time.Second, sameBody: true,
			b: backends{
				http: httpWith(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }),
				grpc: grpcWith(func(s *mockmodel.Server) { s.Respond([]byte(`{}`)) }),
			},
		},
		{
			name: "upstream down", topic: "resp", timeout: time.Second, sameBody: true,
			b: backends{
				http: func(t *testing.T) transport.Upstream {
					srv := httptest.NewServer(http.NotFoundHandler())
					conf := upstreamConf(t, srv, "")
					srv.Close()
					return transport.NewHTTPUpstream(conf)
				},
				grpc: func(t *testing.T) transport.Upstream {
					return grpcUpstream(t, grpcConf(t, deadAddr(), schemas.ServiceTLS{}))
				},
			},
		},
		{
			name: "timeout", topic: "resp", timeout: 150 * time.Millisecond, sameBody: true,
			b: backends{
				http: httpWith(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					<-r.Context().Done()
				}),
				grpc: grpcWith(func(s *mockmodel.Server) { s.Delay(time.Hour) }),
			},
		},
		{
			name: "application error", topic: "resp", timeout: 3 * time.Second, sameBody: false,
			b: backends{
				http: httpWith(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":"bad"}`))
				}),
				grpc: grpcWith(func(s *mockmodel.Server) { s.Fail(status.Error(codes.InvalidArgument, "bad")) }),
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			fromHTTP := observe(t, sc.b.http(t), sc.topic, sc.timeout)
			fromGRPC := observe(t, sc.b.grpc(t), sc.topic, sc.timeout)

			if !sc.sameBody {
				fromHTTP.body, fromGRPC.body = "", ""
			}
			assert.Equal(t, fromHTTP, fromGRPC, "HTTP and gRPC must be indistinguishable apart from documented headers")
		})
	}
}

func TestParity_TrackingHeadersAreCarriedOverBothTransports(t *testing.T) {
	for name, up := range map[string]func(*testing.T) transport.Upstream{
		"http": httpWith(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }),
		"grpc": grpcWith(func(s *mockmodel.Server) { s.Respond([]byte(`{}`)) }),
	} {
		t.Run(name, func(t *testing.T) {
			got := observe(t, up(t), "resp", 3*time.Second)

			assert.True(t, got.published)
			assert.Equal(t, "req-1", got.requestID, "the originating request id reaches the response record")
			assert.Equal(t, "client-7", got.clientID)
		})
	}
}

// Headers one transport cannot carry are left out only there; everything else about the record is
// identical over both transports.
func TestParity_HeadersOnlyOneTransportCanCarry(t *testing.T) {
	var httpGot http.Header
	httpUp := httpWith(func(w http.ResponseWriter, r *http.Request) {
		httpGot = r.Header.Clone()
		_, _ = w.Write([]byte(`{}`))
	})(t)
	grpcSrv := mockmodel.Start(t)
	grpcSrv.Respond([]byte(`{}`))
	grpcUp := grpcUpstream(t, grpcConf(t, grpcSrv.Addr, schemas.ServiceTLS{}))

	run := func(up transport.Upstream) outcome {
		q := transport.NewResponseQueue()
		ev, acks := newEvent("resp")
		ev.Values.Body.Headers["note"] = "a\nb"
		ev.Values.Body.Headers["Content-Type"] = "text/plain"
		transport.SendToService(context.Background(), logger(t), up, ev, q, 3*time.Second)
		out := outcome{success: ev.Event.Success, failure: ev.Event.Failure}
		rec := <-q
		out.published, out.requestID, out.clientID, out.body = true,
			rec.Values.Body.Headers[consts.HeaderRequestIdKey], rec.Values.Body.Headers[consts.HeaderClientId], string(rec.Values.Body.Data)
		rec.Ack()
		out.acks = *acks
		return out
	}

	fromHTTP, fromGRPC := run(httpUp), run(grpcUp)

	assert.Equal(t, fromHTTP, fromGRPC)
	assert.Empty(t, httpGot.Get("Note"), "HTTP drops a value with a line feed")
	assert.Equal(t, "application/json", httpGot.Get("Content-Type"), "HTTP leaves the record's Content-Type out")
	details := grpcSrv.Requests()[0].Details
	assert.Equal(t, "a\nb", details["note"], "gRPC carries it in details")
	assert.Equal(t, "text/plain", details["Content-Type"], "gRPC sends Content-Type as data")
}
