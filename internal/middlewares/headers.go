package middlewares

import (
	"net/http"
	"strings"
	"time"

	"bridge-kafka-service/internal/codec"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// HeaderRule defines the default of a tracking header that is absent.
type HeaderRule struct {
	Key          string
	DefaultValue func(existingHeaders map[string]string) string
}

// headerRules defines the rules for system headers.
var headerRules = []HeaderRule{
	{
		Key: consts.HeaderRequestIdKey,
		DefaultValue: func(_ map[string]string) string {
			return codec.NewRequestID()
		},
	},
	{
		Key: consts.HeaderRequestTimeKey,
		DefaultValue: func(_ map[string]string) string {
			return time.Now().UTC().Format("2006-01-02 15:04:05Z")
		},
	},
	{
		Key: consts.HeaderTraceIdKey,
		DefaultValue: func(headers map[string]string) string {
			return headers[consts.HeaderRequestIdKey]
		},
	},
}

// ProcessHeaders applies the tracking-header rules: a header that is absent gets its default. Which
// headers must be present is consts.RequiredHeaders, checked by the entry points.
func ProcessHeaders(header http.Header) map[string]string {
	result := make(map[string]string)

	for _, rule := range headerRules {
		value := header.Get(rule.Key)
		if value == "" && rule.DefaultValue != nil {
			value = rule.DefaultValue(result)
		}
		result[rule.Key] = value
	}

	return result
}

// HeaderHttpHandler processes HTTP headers and sets them as Kafka headers.
func HeaderHttpHandler(header http.Header, body *schemas.RequestBody) {
	for key, value := range ProcessHeaders(header) {
		body.Headers[key] = value
	}

	liftCustomHeaders(header, body.Headers)
}

// missingRequiredHeader returns the first header of consts.RequiredHeaders that header lacks, or "".
func missingRequiredHeader(header http.Header) string {
	for _, name := range consts.RequiredHeaders {
		if header.Get(name) == "" {
			return name
		}
	}
	return ""
}

// liftCustomHeaders copies headers carrying the application's custom prefix into
// dst with the prefix stripped (the key keeps net/http's canonical form).
func liftCustomHeaders(src http.Header, dst map[string]string) {
	prefix := http.CanonicalHeaderKey(consts.CustomHeaderPrefix)
	for k, v := range src {
		if strings.HasPrefix(k, prefix) && len(v) > 0 {
			dst[k[len(prefix):]] = v[0]
		}
	}
}

// setHttpHeaders sets HTTP response headers from the processed headers.
func setHttpHeaders(w http.ResponseWriter, headers map[string]string) {
	w.Header().Set("content-type", "application/json")

	for _, rule := range headerRules {
		if value, exists := headers[rule.Key]; exists {
			w.Header().Set(rule.Key, value)
		}
	}
}

// setKafkaHeaders fills body.Headers with Kafka headers from the processed headers, including the
// optional partition key (x-kafka-key) and every required header under its lowercase wire name.
func setKafkaHeaders(
	requestHeaders http.Header,
	body *schemas.RequestBody,
) {
	for key, value := range ProcessHeaders(requestHeaders) {
		body.Headers[key] = value
	}

	if key := requestHeaders.Get(consts.HeaderKafkaKey); key != "" {
		body.Headers[consts.HeaderKafkaKey] = key
	}

	for _, name := range consts.RequiredHeaders {
		if value := requestHeaders.Get(name); value != "" {
			body.Headers[strings.ToLower(name)] = value
		}
	}

	liftCustomHeaders(requestHeaders, body.Headers)
}
