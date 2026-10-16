package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/internal/transport/modelrpc"
	"bridge-kafka-service/testutil/testcerts"
)

// start runs the example server with the given TLS flags and returns its address.
func start(t *testing.T, certFile, keyFile, clientCA string) string {
	t.Helper()
	opts, err := serverOptions(certFile, keyFile, clientCA)
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer(opts...)
	modelrpc.RegisterBaseModelInterfaceServer(s, server{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// call uses the bridge's own gRPC upstream, configured like a consumer stanza.
func call(t *testing.T, addr string, svcTLS schemas.ServiceTLS) (transport.UpstreamResponse, error) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	conf := &schemas.ConsumerConfig{Service: schemas.HostConfig{Name: host, Port: port, Protocol: "grpc", TLS: svcTLS}}
	require.NoError(t, config.PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{conf}}))
	up, err := transport.NewUpstream(conf)
	require.NoError(t, err)
	t.Cleanup(func() { _ = up.(interface{ Close() error }).Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return up.Call(ctx, transport.UpstreamRequest{
		Body:    []byte(`{"params":[1,2,3]}`),
		Headers: map[string]string{"x-request-id": "abc-123"},
	})
}

func TestExamplePlaintext(t *testing.T) {
	resp, err := call(t, start(t, "", "", ""), schemas.ServiceTLS{})

	require.NoError(t, err)
	assert.JSONEq(t, `{"echo":{"params":[1,2,3]},"request_id":"abc-123","service":"example-grpc-model"}`, string(resp.Body))
	assert.Equal(t, "example-grpc-model", resp.Header.Get("X-Client-Id"), "response metadata becomes a response header")
}

func TestExampleTLSAndMutualTLS(t *testing.T) {
	b := testcerts.New(t, t.TempDir())

	t.Run("tls", func(t *testing.T) {
		addr := start(t, b.ServerCertPath, b.ServerKeyPath, "")
		resp, err := call(t, addr, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath})
		require.NoError(t, err)
		assert.Contains(t, string(resp.Body), `"request_id":"abc-123"`)
	})

	t.Run("mtls", func(t *testing.T) {
		addr := start(t, b.ServerCertPath, b.ServerKeyPath, b.CAPath)
		resp, err := call(t, addr, schemas.ServiceTLS{
			Mode: "mtls", CAPath: b.CAPath, CertPath: b.ClientCertPath, KeyPath: b.ClientKeyPath,
		})
		require.NoError(t, err)
		assert.Contains(t, string(resp.Body), `"service":"example-grpc-model"`)

		_, err = call(t, addr, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath})
		assert.Error(t, err, "a client without a certificate is rejected")
	})
}

func TestExampleNonJSONPayloadIsEchoedAsString(t *testing.T) {
	resp, err := server{}.Predict(context.Background(), &modelrpc.ModelInterfaceRequest{Data: []byte("plain")})
	require.NoError(t, err)
	assert.JSONEq(t, `{"echo":"plain","request_id":"","service":"example-grpc-model"}`, string(resp.GetData()))
}

func TestServerOptionsRejectsBadFlags(t *testing.T) {
	_, err := serverOptions("", "", "ca.pem")
	assert.ErrorContains(t, err, "requires -tls-cert")
	_, err = serverOptions("/nope.pem", "/nope.key", "")
	assert.ErrorContains(t, err, "load server certificate")
}
