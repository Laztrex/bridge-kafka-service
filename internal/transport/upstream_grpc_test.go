package transport_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/mockmodel"
	"bridge-kafka-service/testutil/testcerts"
)

// grpcConf builds a gRPC consumer config for addr and applies the TLS settings
// exactly as startup would.
func grpcConf(t *testing.T, addr string, svcTLS schemas.ServiceTLS) *schemas.ConsumerConfig {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	conf := &schemas.ConsumerConfig{Service: schemas.HostConfig{Name: host, Port: port, Protocol: "grpc", TLS: svcTLS}}
	require.NoError(t, config.PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{conf}}))
	return conf
}

func grpcUpstream(t *testing.T, conf *schemas.ConsumerConfig) transport.Upstream {
	t.Helper()
	up, err := transport.NewUpstream(conf)
	require.NoError(t, err)
	if c, ok := up.(interface{ Close() error }); ok {
		t.Cleanup(func() { _ = c.Close() })
	}
	return up
}

func predict(t *testing.T, up transport.Upstream, req transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return up.Call(ctx, req)
}

func TestGRPCUpstream_MapsRequestAndResponse(t *testing.T) {
	srv := mockmodel.Start(t)
	srv.Respond([]byte(`{"answer":42}`))
	srv.RespondMetadata(metadata.Pairs("x-model", "v1"), metadata.Pairs("x-trailer", "t1"))
	up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))

	resp, err := predict(t, up, transport.UpstreamRequest{
		Body:    []byte(`{"q":1}`),
		Headers: map[string]string{consts.HeaderRequestIdKey: "req-1", "x-custom": "v"},
	})

	require.NoError(t, err)
	assert.JSONEq(t, `{"answer":42}`, string(resp.Body))
	assert.Equal(t, "v1", resp.Header.Get("X-Model"), "header metadata becomes a response header")
	assert.Equal(t, "t1", resp.Header.Get("X-Trailer"), "trailer metadata becomes a response header")
	assert.Empty(t, resp.Header.Get("Grpc-Status"))

	got := srv.Requests()
	require.Len(t, got, 1)
	assert.JSONEq(t, `{"q":1}`, string(got[0].Data), "payload travels as data")
	assert.Equal(t, map[string]string{consts.HeaderRequestIdKey: "req-1", "x-custom": "v"}, got[0].Details, "every record header travels in details")
	assert.Equal(t, []string{"req-1"}, got[0].Metadata.Get(consts.HeaderRequestIdKey), "x-request-id is also sent as metadata")
}

func TestGRPCUpstream_NonASCIIRequestIdStaysInDetailsOnly(t *testing.T) {
	srv := mockmodel.Start(t)
	up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))

	_, err := predict(t, up, transport.UpstreamRequest{
		Body: []byte(`{}`), Headers: map[string]string{consts.HeaderRequestIdKey: "ïdéntïfïér"},
	})

	require.NoError(t, err, "an id that is not valid metadata must not break the call")
	got := srv.Requests()[0]
	assert.Equal(t, "ïdéntïfïér", got.Details[consts.HeaderRequestIdKey])
	assert.Empty(t, got.Metadata.Get(consts.HeaderRequestIdKey))
}

func TestGRPCUpstream_TransportFailureStatuses(t *testing.T) {
	t.Run("service down", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := l.Addr().String()
		require.NoError(t, l.Close())

		_, err = predict(t, grpcUpstream(t, grpcConf(t, addr, schemas.ServiceTLS{})), transport.UpstreamRequest{Body: []byte(`{}`)})

		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("deadline exceeded", func(t *testing.T) {
		srv := mockmodel.Start(t)
		srv.Delay(2 * time.Second)
		up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := up.Call(ctx, transport.UpstreamRequest{Body: []byte(`{}`)})

		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	})

	t.Run("cancelled", func(t *testing.T) {
		srv := mockmodel.Start(t)
		up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := up.Call(ctx, transport.UpstreamRequest{Body: []byte(`{}`)})

		assert.Equal(t, codes.Canceled, status.Code(err))
	})
}

func TestGRPCUpstream_ApplicationStatusesAreAnswers(t *testing.T) {
	for _, code := range []codes.Code{
		codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.FailedPrecondition,
		codes.ResourceExhausted, codes.Unimplemented, codes.Internal, codes.Unknown,
	} {
		t.Run(code.String(), func(t *testing.T) {
			srv := mockmodel.Start(t)
			srv.Fail(status.Error(code, "service says no"))
			up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))

			resp, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})

			require.NoError(t, err, "the service answered: not a transport failure")
			assert.Empty(t, resp.Body)
			assert.Equal(t, code.String(), resp.Header.Get("Grpc-Status"))
			assert.Equal(t, "service says no", resp.Header.Get("Grpc-Message"))
		})
	}
}

func TestGRPCUpstream_MessageSizeLimit(t *testing.T) {
	srv := mockmodel.Start(t)
	up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))

	t.Run("10 MiB response is received", func(t *testing.T) {
		srv.Respond([]byte(strings.Repeat("a", 10<<20)))
		resp, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		require.NoError(t, err)
		assert.Len(t, resp.Body, 10<<20)
	})

	t.Run("response over 16 MiB is a ResourceExhausted answer", func(t *testing.T) {
		srv.Respond([]byte(strings.Repeat("a", 17<<20)))
		resp, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		require.NoError(t, err)
		assert.Empty(t, resp.Body)
		assert.Equal(t, "ResourceExhausted", resp.Header.Get("Grpc-Status"))
	})

	t.Run("request over 16 MiB is not sent", func(t *testing.T) {
		before := len(srv.Requests())
		resp, err := predict(t, up, transport.UpstreamRequest{Body: []byte(strings.Repeat("a", 17<<20))})
		require.NoError(t, err)
		assert.Equal(t, "ResourceExhausted", resp.Header.Get("Grpc-Status"))
		assert.Len(t, srv.Requests(), before, "nothing reached the service")
	})
}

func TestGRPCUpstream_ServiceStartsAfterTheBridge(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	up, err := transport.NewUpstream(grpcConf(t, addr, schemas.ServiceTLS{}))
	require.NoError(t, err, "an unreachable service must not block creating the upstream")
	defer up.(interface{ Close() error }).Close()

	_, err = predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
	assert.Equal(t, codes.Unavailable, status.Code(err), "calls fail while the service is absent")

	srv := mockmodel.Start(t, mockmodel.WithAddr(addr))
	srv.Respond([]byte(`{"back":true}`))
	require.Eventually(t, func() bool {
		resp, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		return err == nil && strings.Contains(string(resp.Body), "back")
	}, 15*time.Second, 200*time.Millisecond, "calls succeed once the service appears")
}

func TestGRPCUpstream_TLS(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	other := testcerts.New(t, t.TempDir())
	srv := mockmodel.Start(t, mockmodel.WithTLS(b.ServerConfig(false)))
	srv.Respond([]byte(`{"secure":true}`))

	t.Run("trusted CA", func(t *testing.T) {
		up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath}))
		resp, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		require.NoError(t, err)
		assert.JSONEq(t, `{"secure":true}`, string(resp.Body))
	})

	t.Run("untrusted server is a transport failure", func(t *testing.T) {
		up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{Mode: "tls", CAPath: other.CAPath}))
		_, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("plaintext client cannot use a TLS server", func(t *testing.T) {
		up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{}))
		_, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("server_name override is what gets verified", func(t *testing.T) {
		good := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath, ServerName: "localhost"}))
		_, err := predict(t, good, transport.UpstreamRequest{Body: []byte(`{}`)})
		assert.NoError(t, err)

		bad := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath, ServerName: "wrong.example"}))
		_, err = predict(t, bad, transport.UpstreamRequest{Body: []byte(`{}`)})
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("skip_verify accepts an untrusted server", func(t *testing.T) {
		up := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{Mode: "tls", SkipVerify: true}))
		_, err := predict(t, up, transport.UpstreamRequest{Body: []byte(`{}`)})
		assert.NoError(t, err)
	})
}

func TestGRPCUpstream_MutualTLS(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	srv := mockmodel.Start(t, mockmodel.WithTLS(b.ServerConfig(true)))

	withCert := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{
		Mode: "mtls", CAPath: b.CAPath, CertPath: b.ClientCertPath, KeyPath: b.ClientKeyPath,
	}))
	_, err := predict(t, withCert, transport.UpstreamRequest{Body: []byte(`{}`)})
	assert.NoError(t, err, "a client certificate signed by the CA is accepted")

	withoutCert := grpcUpstream(t, grpcConf(t, srv.Addr, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath}))
	_, err = predict(t, withoutCert, transport.UpstreamRequest{Body: []byte(`{}`)})
	assert.Error(t, err, "a client without a certificate is rejected")
}
