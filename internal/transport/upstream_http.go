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

type httpUpstream struct {
	uri    string
	client *http.Client
}

// NewHTTPUpstream returns the HTTP Upstream: a JSON POST to the service address (https with TLS),
// bounded by the call's context rather than a client timeout.
func NewHTTPUpstream(conf *schemas.ConsumerConfig) Upstream {
	base := http.DefaultTransport
	if conf.UpstreamTLS != nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.TLSClientConfig = conf.UpstreamTLS.Clone()
		base = t
	}
	return &httpUpstream{
		uri:    conf.Service.URI(),
		client: &http.Client{Transport: middlewares.LoggerTransportRoundTripper{Proxy: base}},
	}
}

func (u *httpUpstream) Call(ctx context.Context, req UpstreamRequest) (UpstreamResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.uri, bytes.NewReader(req.Body))
	if err != nil {
		return UpstreamResponse{}, fmt.Errorf("failed create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := u.client.Do(httpReq)
	if err != nil {
		return UpstreamResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	log.Info().Str(consts.HeaderRequestIdKey, req.Headers[consts.HeaderRequestIdKey]).
		Msgf("response received: status %d", resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return UpstreamResponse{}, fmt.Errorf("failed to read response body: %w", err)
	}
	return UpstreamResponse{Body: body, Header: resp.Header}, nil
}
