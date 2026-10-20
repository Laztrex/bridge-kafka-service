package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/middlewares"
	"bridge-kafka-service/internal/schemas"
)

// httpMaxResponseBytes caps the response body read from an HTTP upstream, the same limit the gRPC
// client applies, so a misbehaving service cannot make the process allocate without bound.
const httpMaxResponseBytes = grpcMaxMessageBytes

type httpUpstream struct {
	uri    string
	client *http.Client
}

// NewHTTPUpstream returns the HTTP Upstream: a JSON POST to the service address (https with TLS),
// bounded by the call's context rather than a client timeout. It uses its own
// connection pool (a clone of http.DefaultTransport) sized to the pipeline's
// concurrency limit, so concurrent calls reuse connections instead of
// reconnecting, and pipelines never share a pool.
func NewHTTPUpstream(conf *schemas.ConsumerConfig) Upstream {
	t := http.DefaultTransport.(*http.Transport).Clone()
	limit := concurrencyLimit(conf)
	t.MaxIdleConns = limit
	t.MaxIdleConnsPerHost = limit
	if conf.UpstreamTLS != nil {
		t.TLSClientConfig = conf.UpstreamTLS.Clone()
	}
	return &httpUpstream{
		uri:    conf.Service.URI(),
		client: &http.Client{Transport: middlewares.LoggerTransportRoundTripper{Proxy: t}},
	}
}

func (u *httpUpstream) Call(ctx context.Context, req UpstreamRequest) (UpstreamResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.uri, bytes.NewReader(req.Body))
	if err != nil {
		return UpstreamResponse{}, fmt.Errorf("failed create request: %w", err)
	}
	setRecordHeaders(httpReq.Header, req.Headers)

	resp, err := u.client.Do(httpReq)
	if err != nil {
		return UpstreamResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	log.Info().Str(consts.HeaderRequestIdKey, req.Headers[consts.HeaderRequestIdKey]).
		Msgf("response received: status %d", resp.StatusCode)

	body, err := io.ReadAll(io.LimitReader(resp.Body, httpMaxResponseBytes+1))
	if err != nil {
		return UpstreamResponse{}, fmt.Errorf("failed to read response body: %w", err)
	}
	if len(body) > httpMaxResponseBytes {
		return UpstreamResponse{}, permanentError{fmt.Errorf("response body of %s exceeds %d bytes", u.uri, httpMaxResponseBytes)}
	}
	return UpstreamResponse{Body: body, Header: resp.Header}, nil
}
