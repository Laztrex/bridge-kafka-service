package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"bridge-kafka-service/internal/schemas"
)

// UpstreamRequest is what every transport sends to the business service: the
// normalised JSON payload plus the Kafka record headers.
type UpstreamRequest struct {
	Body    json.RawMessage
	Headers map[string]string
}

// UpstreamResponse is what every transport hands back: the raw response body and
// the response headers. Header keeps net/http's canonical form so that header
// handling is identical whatever the transport (a gRPC transport converts its
// metadata into it).
type UpstreamResponse struct {
	Body   []byte
	Header http.Header
}

// Upstream performs one call to the business service; everything around it is transport-neutral.
// An error means a transport-level failure; cancelling ctx must abort the call.
type Upstream interface {
	Call(ctx context.Context, req UpstreamRequest) (UpstreamResponse, error)
}

// NewUpstream builds the Upstream selected by the stanza's service.protocol
// (http by default). Configuration is validated at startup, so an error here
// means a protocol that has no implementation.
func NewUpstream(conf *schemas.ConsumerConfig) (Upstream, error) {
	switch p := conf.Service.EffectiveProtocol(); p {
	case schemas.ProtocolHTTP:
		return NewHTTPUpstream(conf), nil
	case schemas.ProtocolGRPC:
		return NewGRPCUpstream(conf)
	default:
		return nil, fmt.Errorf("unsupported upstream protocol %q", p)
	}
}
