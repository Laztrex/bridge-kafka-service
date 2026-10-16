package observability

import (
	"time"

	"github.com/rs/zerolog/log"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	data_accesslog "github.com/envoyproxy/go-control-plane/envoy/data/accesslog/v3"
	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

const (
	headerXForwardedFor  = "x-forwarded-for"
	fallbackForwardedFor = "controller-kafka"
	upstreamClusterLocal = "kafka-controller-cluster"
)

// accessLogTypeToEnum maps the controller's string taxonomy onto the numeric
// envoy AccessLogType enum expected by audit-controller v1.3.0 receivers.
var accessLogTypeToEnum = map[string]data_accesslog.AccessLogType{
	consts.AccessLogTypeDownstreamStart:            data_accesslog.AccessLogType_DownstreamStart,
	consts.AccessLogTypeDownstreamEnd:              data_accesslog.AccessLogType_DownstreamEnd,
	consts.AccessLogTypeBusinessRequestTransferred: data_accesslog.AccessLogType_DownstreamEnd,
}

// resolveForwardedFor returns the trimmed x-forwarded-for header value when it
// is present and non-empty, otherwise a stable local placeholder so that the
// receiver-side Validate() cannot drop the event with metric user_node_is_empty.
func resolveForwardedFor(headers map[string]string) string {
	if headers != nil {
		if v := headers[headerXForwardedFor]; v != "" {
			return v
		}
	}
	return fallbackForwardedFor
}

// LogAuditEventWithLatency dispatches an Envoy-compatible ALS audit entry with the time since
// startedAt as its duration (at least 1ns: the receiver rejects a zero duration).
func LogAuditEventWithLatency(
	transaction *FileTransactionLogger,
	eventName string,
	requestId string,
	body *schemas.RequestBody,
	accessLogType string,
	path string,
	startedAt time.Time,
) {

	var requestBody, responseBody string
	if body != nil && body.Data != nil {
		payload := string(body.Data)
		switch accessLogType {
		case consts.AccessLogTypeDownstreamStart:
			requestBody = payload
			responseBody = ""
		case consts.AccessLogTypeDownstreamEnd, consts.AccessLogTypeBusinessRequestTransferred:
			requestBody = ""
			responseBody = payload
		default:
			requestBody = payload
			responseBody = payload
		}
	} else {
		requestBody = "{}"
		responseBody = "{}"
	}

	msg := &accesslog.StreamAccessLogsMessage{}

	now := transaction.now()

	var logName, metadataKey string
	switch accessLogType {
	case consts.AccessLogTypeDownstreamStart:
		logName = "envoy-ingress-als"
		metadataKey = "envoy.ingress.audit-controller.body.lua"
	case consts.AccessLogTypeDownstreamEnd, consts.AccessLogTypeBusinessRequestTransferred:
		logName = "envoy-egress-als"
		metadataKey = "envoy.egress.audit-controller.body.lua"
	default:
		logName = "envoy-ingress-als"
		metadataKey = "envoy.ingress.audit-controller.body.lua"
	}

	elapsed := now.Sub(startedAt)
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}

	headers := make(map[string]interface{})
	headers["x-trace-id"] = requestId
	headers["x-request-id"] = requestId

	var bodyHeaders map[string]string
	if body != nil {
		bodyHeaders = body.Headers
	}

	forwardedFor := resolveForwardedFor(bodyHeaders)
	if forwardedFor != "" {
		headers["x-forwarded-for"] = forwardedFor
	}

	for k, v := range bodyHeaders {
		headers[k] = v
	}

	headersStruct, err := structpb.NewStruct(headers)
	if err != nil {
		log.Error().Err(err).Interface("headers", headers).Msg("Failed to create headers struct")
		headersStruct, _ = structpb.NewStruct(map[string]interface{}{})
	}

	emptyTrailers, _ := structpb.NewStruct(map[string]interface{}{})

	metadataFields := map[string]interface{}{
		"request.headers":   headersStruct.AsMap(),
		"request.body":      requestBody,
		"request.trailers":  emptyTrailers.AsMap(),
		"response.headers":  headersStruct.AsMap(),
		"response.body":     responseBody,
		"response.trailers": emptyTrailers.AsMap(),
	}

	metadataStruct, err := structpb.NewStruct(metadataFields)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create metadata struct")
		metadataStruct, _ = structpb.NewStruct(map[string]interface{}{})
	}

	filterMetadata := map[string]*structpb.Struct{
		metadataKey: metadataStruct,
	}

	httpEntry := &data_accesslog.HTTPAccessLogEntry{
		CommonProperties: &data_accesslog.AccessLogCommon{
			StartTime:       timestamppb.New(now),
			Duration:        durationpb.New(elapsed),
			UpstreamCluster: upstreamClusterLocal,
			AccessLogType:   lookupAccessLogType(accessLogType),
			Metadata: &core.Metadata{
				FilterMetadata: filterMetadata,
			},
			StreamId: requestId,
		},
		Request: &data_accesslog.HTTPRequestProperties{
			RequestMethod:       core.RequestMethod_POST,
			Scheme:              "http",
			Authority:           "controller-kafka",
			Path:                path,
			UserAgent:           "controller-kafka/1.0",
			ForwardedFor:        forwardedFor,
			RequestBodyBytes:    uint64(len(requestBody)),
			RequestHeadersBytes: uint64(len(headers)),
		},
		Response: &data_accesslog.HTTPResponseProperties{
			ResponseCode:         wrapperspb.UInt32(200),
			ResponseCodeDetails:  "OK",
			ResponseHeadersBytes: uint64(len(headers)),
			ResponseBodyBytes:    uint64(len(responseBody)),
		},
		ProtocolVersion: data_accesslog.HTTPAccessLogEntry_HTTP2,
	}

	msg.LogEntries = &accesslog.StreamAccessLogsMessage_HttpLogs{
		HttpLogs: &accesslog.StreamAccessLogsMessage_HTTPAccessLogEntries{
			LogEntry: []*data_accesslog.HTTPAccessLogEntry{httpEntry},
		},
	}

	msg.Identifier = &accesslog.StreamAccessLogsMessage_Identifier{
		Node: &core.Node{
			Id:      "controller-kafka",
			Cluster: upstreamClusterLocal,
		},
		LogName: logName,
	}

	log.Debug().Str("log_name", logName).Str("event", eventName).Str("access_log_type", accessLogType).
		Msg("sending audit message")

	transaction.CloseAudit(msg)
}

// lookupAccessLogType returns the numeric envoy enum for the supplied string
// constant. Unknown strings fall back to NotSet rather than the protobuf default,
// matching audit-controller's expectation of explicit values.
func lookupAccessLogType(s string) data_accesslog.AccessLogType {
	if t, ok := accessLogTypeToEnum[s]; ok {
		return t
	}
	return data_accesslog.AccessLogType_NotSet
}
