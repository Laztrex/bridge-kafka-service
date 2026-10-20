package middlewares

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

func TestProcessHeaders_GeneratesMissingAndKeepsGiven(t *testing.T) {
	h := http.Header{}
	got := ProcessHeaders(h)
	assert.Len(t, got[consts.HeaderRequestIdKey], 36)
	assert.Equal(t, got[consts.HeaderRequestIdKey], got[consts.HeaderTraceIdKey], "trace id mirrors request id")
	assert.NotEmpty(t, got[consts.HeaderRequestTimeKey])

	h.Set(consts.HeaderRequestIdKey, "given")
	got = ProcessHeaders(h)
	assert.Equal(t, "given", got[consts.HeaderRequestIdKey])
	assert.Equal(t, "given", got[consts.HeaderTraceIdKey])
}

func TestSetKafkaHeaders_StripsCustomPrefix(t *testing.T) {
	h := http.Header{}
	h.Set(consts.CustomHeaderPrefix+"tenant", "acme")
	h.Set("X-Other", "ignored")
	body := &schemas.RequestBody{Headers: map[string]string{}}

	setKafkaHeaders(h, body)

	assert.Equal(t, "acme", body.Headers["Tenant"])
	assert.NotContains(t, body.Headers, "X-Other")
	assert.NotEmpty(t, body.Headers[consts.HeaderRequestIdKey])
}

func TestHeaderHttpHandler_LiftsCustomHeadersFromResponse(t *testing.T) {
	header := http.Header{}
	header.Set(consts.HeaderRequestIdKey, "rid")
	header.Set(consts.CustomHeaderPrefix+"k", "v")
	body := &schemas.RequestBody{Headers: map[string]string{}}

	HeaderHttpHandler(header, body)

	assert.Equal(t, "rid", body.Headers[consts.HeaderRequestIdKey])
	assert.Equal(t, "v", body.Headers["K"])
}

func TestSetHttpHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	setHttpHeaders(w, map[string]string{consts.HeaderRequestIdKey: "rid"})
	assert.Equal(t, "rid", w.Header().Get(consts.HeaderRequestIdKey))
	assert.Equal(t, "application/json", w.Header().Get("content-type"))
}

func TestLoggerRoundTripper_AddsRequestIdAndWrapsErrors(t *testing.T) {
	var seen string
	rt := LoggerTransportRoundTripper{Proxy: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.Header.Get(consts.HeaderRequestIdKey)
		return &http.Response{StatusCode: 500, Header: http.Header{}, Body: http.NoBody}, nil
	})}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/x", nil)

	res, err := rt.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode)
	assert.NotEmpty(t, seen)

	bad := LoggerTransportRoundTripper{Proxy: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, assert.AnError })}
	_, err = bad.RoundTrip(httptest.NewRequest(http.MethodPost, "http://127.0.0.1/x", nil))
	assert.ErrorContains(t, err, "unavailable")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSetKafkaHeaders_CarriesThePartitionKey(t *testing.T) {
	h := http.Header{}
	h.Set(consts.HeaderKafkaKey, "user-42")
	body := &schemas.RequestBody{Headers: map[string]string{}}

	setKafkaHeaders(h, body)

	assert.Equal(t, "user-42", body.Headers[consts.HeaderKafkaKey])

	empty := &schemas.RequestBody{Headers: map[string]string{}}
	setKafkaHeaders(http.Header{}, empty)
	assert.NotContains(t, empty.Headers, consts.HeaderKafkaKey)
}
