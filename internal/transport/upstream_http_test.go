package transport_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/testcerts"
)

// httpsServer starts an HTTPS server presenting the bundle's server certificate.
func httpsServer(t *testing.T, b *testcerts.Bundle, requireClient bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"secure":true}`))
	}))
	srv.TLS = b.ServerConfig(requireClient)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// tlsConf points a consumer config at srv and applies the TLS settings exactly
// as startup would (config.PrepareUpstreams).
func tlsConf(t *testing.T, srv *httptest.Server, svcTLS schemas.ServiceTLS) *schemas.ConsumerConfig {
	t.Helper()
	conf := upstreamConf(t, srv, "")
	conf.Service.TLS = svcTLS
	require.NoError(t, config.PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{conf}}))
	return conf
}

func call(t *testing.T, conf *schemas.ConsumerConfig) (transport.UpstreamResponse, error) {
	t.Helper()
	return transport.NewHTTPUpstream(conf).Call(context.Background(), transport.UpstreamRequest{Body: []byte(`{}`)})
}

func TestHTTPUpstream_TLSWithTrustedCA(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	srv := httpsServer(t, b, false)
	conf := tlsConf(t, srv, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath})

	assert.Contains(t, conf.Service.URI(), "https://")
	resp, err := call(t, conf)

	require.NoError(t, err)
	assert.JSONEq(t, `{"secure":true}`, string(resp.Body))
}

func TestHTTPUpstream_UntrustedCertificateIsATransportFailure(t *testing.T) {
	server := testcerts.New(t, t.TempDir())
	other := testcerts.New(t, t.TempDir())
	srv := httpsServer(t, server, false)
	conf := tlsConf(t, srv, schemas.ServiceTLS{Mode: "tls", CAPath: other.CAPath})

	_, err := call(t, conf)

	assert.Error(t, err)
	assert.ErrorContains(t, err, "unavailable")
}

// The TLS server answers plaintext with HTTP 400: not a transport error, but the service is never
// reached and the connection is not silently upgraded.
func TestHTTPUpstream_PlainClientCannotTalkToTLSServer(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	srv := httpsServer(t, b, false)
	conf := upstreamConf(t, srv, "")

	resp, err := call(t, conf)

	require.NoError(t, err)
	assert.NotContains(t, string(resp.Body), "secure")
	assert.Contains(t, string(resp.Body), "HTTPS")
}

func TestHTTPUpstream_ServerNameOverride(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	srv := httpsServer(t, b, false)

	good := tlsConf(t, srv, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath, ServerName: "localhost"})
	_, err := call(t, good)
	assert.NoError(t, err, "the certificate is valid for localhost")

	bad := tlsConf(t, srv, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath, ServerName: "wrong.example"})
	_, err = call(t, bad)
	assert.Error(t, err, "the override is what gets verified")
}

func TestHTTPUpstream_SkipVerifyAcceptsAnyCertificate(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	srv := httpsServer(t, b, false)
	conf := tlsConf(t, srv, schemas.ServiceTLS{Mode: "tls", SkipVerify: true})

	_, err := call(t, conf)

	assert.NoError(t, err)
}

func TestHTTPUpstream_MutualTLS(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	srv := httpsServer(t, b, true)

	withCert := tlsConf(t, srv, schemas.ServiceTLS{
		Mode: "mtls", CAPath: b.CAPath, CertPath: b.ClientCertPath, KeyPath: b.ClientKeyPath,
	})
	resp, err := call(t, withCert)
	require.NoError(t, err)
	assert.JSONEq(t, `{"secure":true}`, string(resp.Body))

	withoutCert := tlsConf(t, srv, schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath})
	_, err = call(t, withoutCert)
	assert.Error(t, err, "a client without a certificate is rejected")
}

func TestHTTPUpstream_SendsBodyAndHeadersAndReturnsResponse(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotMethod, gotPath, gotBody, gotHeader = r.Method, r.URL.Path, string(b), r.Header.Get("x-custom")
		w.Header().Set("X-Reply", "yes")
		_, _ = w.Write([]byte(`{"ok":1}`))
	}))
	defer srv.Close()

	resp, err := transport.NewHTTPUpstream(upstreamConf(t, srv, "")).Call(context.Background(), transport.UpstreamRequest{
		Body:    []byte(`{"q":1}`),
		Headers: map[string]string{"x-custom": "v", consts.HeaderRequestIdKey: "r1"},
	})

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/predict", gotPath)
	assert.JSONEq(t, `{"q":1}`, gotBody)
	assert.Equal(t, "v", gotHeader)
	assert.JSONEq(t, `{"ok":1}`, string(resp.Body))
	assert.Equal(t, "yes", resp.Header.Get("X-Reply"))
}

// Frameworks such as FastAPI read a body without Content-Type as plain bytes and
// reject it, so the JSON body is always labelled application/json; a record header
// cannot override it.
func TestHTTPUpstream_ContentType(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Content-Type")
	}))
	defer srv.Close()
	up := transport.NewHTTPUpstream(upstreamConf(t, srv, ""))

	for _, tc := range []struct {
		headers map[string]string
		want    string
	}{
		{nil, "application/json"},
		{map[string]string{"content-type": "application/vnd.api+json"}, "application/json"},
		{map[string]string{"Content-Type": "text/plain"}, "application/json"},
	} {
		if _, err := up.Call(context.Background(), transport.UpstreamRequest{Body: []byte(`{}`), Headers: tc.headers}); err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("headers %v: Content-Type %q, want %q", tc.headers, got, tc.want)
		}
	}
}

func TestHTTPUpstream_NonSuccessStatusIsStillAResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()

	resp, err := transport.NewHTTPUpstream(upstreamConf(t, srv, "")).Call(context.Background(), transport.UpstreamRequest{})

	require.NoError(t, err, "an HTTP error status is an answer, not a transport failure")
	assert.JSONEq(t, `{"error":"boom"}`, string(resp.Body))
}

func TestHTTPUpstream_FailuresAreErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	conf := upstreamConf(t, srv, "")

	t.Run("timeout via context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := transport.NewHTTPUpstream(conf).Call(ctx, transport.UpstreamRequest{})
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("cancelled context aborts the call", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := transport.NewHTTPUpstream(conf).Call(ctx, transport.UpstreamRequest{})
		assert.ErrorIs(t, err, context.Canceled)
	})

	srv.Close()
	t.Run("upstream down", func(t *testing.T) {
		_, err := transport.NewHTTPUpstream(conf).Call(context.Background(), transport.UpstreamRequest{})
		assert.ErrorContains(t, err, "unavailable")
	})
}

func TestHTTPUpstream_InvalidAddress(t *testing.T) {
	conf := &schemas.ConsumerConfig{Service: schemas.HostConfig{Name: "bad host", Port: "x", Path: "/"}}
	_, err := transport.NewHTTPUpstream(conf).Call(context.Background(), transport.UpstreamRequest{})
	assert.Error(t, err)
}

// bodyServer answers every call with n bytes of body.
func bodyServer(t *testing.T, n int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.CopyN(w, zeroReader{}, int64(n))
	}))
	t.Cleanup(srv.Close)
	return srv
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestHTTPUpstream_ResponseOverTheLimitFailsTheCall(t *testing.T) {
	srv := bodyServer(t, transport.HTTPMaxResponseBytes+1)

	_, err := call(t, upstreamConf(t, srv, ""))

	require.Error(t, err)
	assert.ErrorContains(t, err, "exceeds")
	assert.True(t, transport.IsPermanent(err), "an oversized body depends on the record, not on the network")
}

func TestHTTPUpstream_ResponseOverTheLimitIsNotRetried(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.CopyN(w, zeroReader{}, int64(transport.HTTPMaxResponseBytes+1))
	}))
	t.Cleanup(srv.Close)
	conf := upstreamConf(t, srv, "")
	ev, acks := newEvent("")

	transport.SendToServiceRetrying(context.Background(), logger(t), transport.NewHTTPUpstream(conf), ev, nil, time.Second, transport.NewRetryState())

	assert.Equal(t, int32(1), calls.Load(), "the call is made once")
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks, "the offset is settled")
}

func TestHTTPUpstream_ResponseAtTheLimitIsRead(t *testing.T) {
	srv := bodyServer(t, transport.HTTPMaxResponseBytes)

	resp, err := call(t, upstreamConf(t, srv, ""))

	require.NoError(t, err)
	assert.Len(t, resp.Body, transport.HTTPMaxResponseBytes)
}
