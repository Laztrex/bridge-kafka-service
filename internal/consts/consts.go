package consts

import (
	"strings"
	"time"
)

// Server defaults, config file locations and the default paths of the run and metrics logs.
const (
	EnvFile          = ".env"
	EnvFileDirectory = "."

	ControllerHost       = "0.0.0.0"
	ControllerHTTPPort   = "8081"
	ControllerGRPCPort   = "9081"
	ControllerVersionAPI = "v1"

	DefaultMaxConn = 10

	ConfPathConn     = "configs/config_app.yaml"
	ConfPathConsumer = "configs/config_consumer.yaml"
	ConfPathProducer = "configs/config_producer.yaml"

	MetricPath = "./"
	LogPath    = "./"
	MetricFile = "app-metrics.log"
	LogFile    = "app.log"
)

// Tracking headers carried by every request and record.
const (
	HeaderRequestIdKey   = "x-request-id"
	HeaderRequestTimeKey = "x-request-time"
	HeaderTraceIdKey     = "x-trace-id"
	HeaderClientId       = "x-client-id"
)

// RequiredHeaders lists the headers a consumed record and a publish request must carry. It is empty
// for now; the tracking headers above are the candidates. A listed header gets no default value.
// Only tests modify it.
var RequiredHeaders = []string{}

// IsRequiredHeader reports whether name is listed in RequiredHeaders, ignoring case.
func IsRequiredHeader(name string) bool {
	for _, r := range RequiredHeaders {
		if strings.EqualFold(r, name) {
			return true
		}
	}
	return false
}

const (
	HeaderKafkaKey         = "x-kafka-key"
	HeaderProduceTopicName = "x-bridge-kafka-produce-topic"
	CustomHeaderPrefix     = "x-bridge-kafka-header-"
)

// Default Kafka TLS material, used when kafka.tls.* paths are unset.
const (
	PathCaCertDefault = "certs/cacert.pem"
	PathCertDefault   = "certs/client_cert.pem"
	PathKeyDefault    = "certs/client_key.pem"
)

// Default upstream service address, used when a consumer's service is not fully set.
const (
	HostServiceDefault     = "webapp"
	PortServiceDefault     = "8080"
	BasePathServiceDefault = "/predict"
)

const DefaultServiceTimeout = 30 * time.Second

type EventType byte

type ContextKeys uint

const (
	EventEmptyProduce EventType = iota
	EventProduce
	EventConsume
	EventRequestService
	EventResponseService
)

const (
	_ ContextKeys = iota
	PublishContextKey
)

const (
	AccessLogTypeDownstreamStart            = "DownstreamStart"
	AccessLogTypeDownstreamEnd              = "DownstreamEnd"
	AccessLogTypeBusinessRequestTransferred = "BusinessRequestTransferred"

	EventNameBusinessRequestReceived    = "BusinessRequestReceived"
	EventNameBusinessRequestTransferred = "BusinessRequestTransferred"
	EventNameBusinessRequestFinished    = "BusinessRequestFinished"
	EventNameBusinessRequestKafka       = "BusinessRequestKafka"
)

// Metric names written to the metrics file. Every name carries the service prefix
// `bridge_kafka_service_` (MET-2); the former `controller_kafka_` and `bridge_service_`
// spellings are gone — see README "Metrics" for the old-to-new table.
const (
	FieldMetricRquid           = "rqUId"
	FieldMetricCpuUsagePercent = MetricPrefix + "pod_cpu_usage"
	FieldMetricRamUsageMb      = MetricPrefix + "pod_memory_usage"
)

// MetricPrefix starts every metric name the service writes.
const MetricPrefix = "bridge_kafka_service_"

// metricName joins the service prefix with an event's metric suffix.
func metricName(event, suffix string) string { return MetricPrefix + event + "_" + suffix }

var MetricsSuccessMapName = map[EventType]string{
	EventProduce:         metricName("produce", "success_total"),
	EventConsume:         metricName("consume", "success_total"),
	EventRequestService:  metricName("request_service", "success_total"),
	EventResponseService: metricName("response_service", "success_total"),
}

var MetricsFailureMapName = map[EventType]string{
	EventProduce:         metricName("produce", "failure_total"),
	EventConsume:         metricName("consume", "failure_total"),
	EventRequestService:  metricName("request_service", "failure_total"),
	EventResponseService: metricName("response_service", "failure_total"),
}

var MetricsRequestMapName = map[EventType]string{
	EventProduce:         metricName("produce", "requests_total"),
	EventConsume:         metricName("consume", "requests_total"),
	EventRequestService:  metricName("request_service", "requests_total"),
	EventResponseService: metricName("response_service", "requests_total"),
}

var MetricsDurationMapName = map[EventType]string{
	EventProduce:         metricName("produce", "duration_ms"),
	EventConsume:         metricName("consume", "duration_ms"),
	EventRequestService:  metricName("request_service", "duration_ms"),
	EventResponseService: metricName("response_service", "duration_ms"),
}

var LogMapEventName = map[EventType]string{
	EventProduce:         "Write to topic",
	EventConsume:         "Read from topic",
	EventRequestService:  "Send to service",
	EventResponseService: "Response from service",
}
