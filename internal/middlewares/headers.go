package middlewares

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"bridge-kafka-service/internal/codec"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// HeaderRule defines how a header is processed.
type HeaderRule struct {
	Key          string
	Required     bool
	DefaultValue func(existingHeaders map[string]string) string
}

// headerRules defines the rules for system headers.
var headerRules = []HeaderRule{
	{
		Key:      consts.HeaderRequestIdKey,
		Required: false,
		DefaultValue: func(_ map[string]string) string {
			return codec.NewRequestID()
		},
	},
	{
		Key:      consts.HeaderRequestTimeKey,
		Required: false,
		DefaultValue: func(_ map[string]string) string {
			return time.Now().UTC().Format("2006-01-02 15:04:05Z")
		},
	},
	{
		Key:      consts.HeaderTraceIdKey,
		Required: false,
		DefaultValue: func(headers map[string]string) string {
			return headers[consts.HeaderRequestIdKey]
		},
	},
}

// ProcessHeaders processes headers according to the rules.
func ProcessHeaders(header http.Header) (map[string]string, error) {
	result := make(map[string]string)

	for _, rule := range headerRules {
		value := header.Get(rule.Key)

		if value == "" {
			if rule.Required {
				return nil, fmt.Errorf("missing required header: %s", rule.Key)
			}
			if rule.DefaultValue != nil {
				value = rule.DefaultValue(result)
			}
		}
		result[rule.Key] = value
	}

	return result, nil
}

// HeaderHttpHandler processes HTTP headers and sets them as Kafka headers.
func HeaderHttpHandler(header http.Header, body *schemas.RequestBody) error {
	headers, err := ProcessHeaders(header)
	if err != nil {
		return err
	}

	for key, value := range headers {
		body.Headers[key] = value
	}

	liftCustomHeaders(header, body.Headers)

	return nil
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
// optional partition key (x-kafka-key).
func setKafkaHeaders(
	requestHeaders http.Header,
	body *schemas.RequestBody,
) {
	processedHeaders, err := ProcessHeaders(requestHeaders)
	if err == nil {
		for key, value := range processedHeaders {
			body.Headers[key] = value
		}
	}

	if key := requestHeaders.Get(consts.HeaderKafkaKey); key != "" {
		body.Headers[consts.HeaderKafkaKey] = key
	}

	liftCustomHeaders(requestHeaders, body.Headers)
}
