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
	drainPublish()
	ev, acks := newEvent(topic)

	transport.SendToService(context.Background(), logger(t), up, ev, timeout)

	out := outcome{success: ev.Event.Success, failure: ev.Event.Failure, acks: *acks}
	select {
	case rec := <-transport.PublishChannel:
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
