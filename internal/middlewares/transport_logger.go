package middlewares

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
)

// LoggerTransportRoundTripper wraps Proxy and logs every upstream HTTP call.
type LoggerTransportRoundTripper struct {
	Proxy http.RoundTripper
}

func (lrt LoggerTransportRoundTripper) RoundTrip(req *http.Request) (res *http.Response, e error) {

	requestId := req.Header.Get(consts.HeaderRequestIdKey)
	if requestId == "" {
		requestId = uuid.New().String()
		req.Header.Set(consts.HeaderRequestIdKey, requestId)
	}

	addrs, err := net.LookupHost(req.URL.Hostname())
	if err != nil {
		log.Error().
			Str("request_id", requestId).
			Str("host", req.URL.Hostname()).
			Err(err).
			Msg("DNS lookup failed")
	} else {
		log.Debug().
			Str("request_id", requestId).
			Str("host", req.URL.Hostname()).
			Strs("addresses", addrs).
			Msg("DNS lookup results")
	}

	log.Debug().
		Str("request_id", requestId).
		Str("method", req.Method).
		Str("url", req.URL.String()).
		Msg("Outgoing HTTP request")

	t := time.Now()

	res, e = lrt.Proxy.RoundTrip(req)
	latency := time.Since(t).Seconds()

	if e != nil {
		log.Error().
			Str("request_id", requestId).
			Str("host", req.URL.Host).
			Str("method", req.Method).
			Str("path", req.URL.Path).
			Float64("latency_seconds", latency).
			Err(e).
			Msg("HTTP request failed")
		return nil, fmt.Errorf("service %v unavailable: %w", req.URL.Host, e)
	}

	if res == nil {
		log.Error().
			Str("request_id", requestId).
			Str("host", req.URL.Host).
			Msg("Empty response without error")
		return nil, fmt.Errorf("service %v unavailable: empty response", req.URL.Host)
	}

	log.Info().
		Str("request_id", requestId).
		Str("host", req.URL.Host).
		Str("method", req.Method).
		Str("path", req.URL.Path).
		Int("status_code", res.StatusCode).
		Float64("latency_seconds", latency).
		Msg("HTTP request completed")

	if res.StatusCode >= 400 {
		responseHeaders := make(map[string]string)
		for k, v := range res.Header {
			responseHeaders[k] = v[0]
		}
		responseHeadersJSON, _ := json.Marshal(responseHeaders)

		log.Debug().
			Str("request_id", requestId).
			Int("status_code", res.StatusCode).
			RawJSON("response_headers", responseHeadersJSON).
			Msg("Error response details")
	}

	return res, nil
}
