package consts

import "time"

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

const (
	HeaderKafkaKey         = "x-kafka-key"
	HeaderProduceTopicName = "x-controller-kafka-produce-topic"
	CustomHeaderPrefix     = "x-controller-kafka-header-"
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

// Metric names written to the metrics file.
const (
	FieldMetricRquid           = "rqUId"
	FieldMetricCpuUsagePercent = "bridge_service_pod_cpu_usage"
	FieldMetricRamUsageMb      = "bridge_service_pod_memory_usage"
)

var MetricsSuccessMapName = map[EventType]string{
	EventProduce:         "controller_kafka_produce_success_total",
	EventConsume:         "controller_kafka_consume_success_total",
	EventRequestService:  "controller_kafka_request_service_success_total",
	EventResponseService: "controller_kafka_response_service_success_total",
}

var MetricsFailureMapName = map[EventType]string{
	EventProduce:         "controller_kafka_produce_failure_total",
	EventConsume:         "controller_kafka_consume_failure_total",
	EventRequestService:  "controller_kafka_request_service_failure_total",
	EventResponseService: "controller_kafka_response_service_failure_total",
}

var MetricsRequestMapName = map[EventType]string{
	EventProduce:         "controller_kafka_produce_requests_total",
	EventConsume:         "controller_kafka_consume_requests_total",
	EventRequestService:  "controller_kafka_request_service_requests_total",
	EventResponseService: "controller_kafka_response_service_requests_total",
}

var MetricsDurationMapName = map[EventType]string{
	EventProduce:         "controller_kafka_produce_duration_ms",
	EventConsume:         "controller_kafka_consume_duration_ms",
	EventRequestService:  "controller_kafka_request_service_duration_ms",
	EventResponseService: "controller_kafka_response_service_duration_ms",
}

var LogMapEventName = map[EventType]string{
	EventProduce:         "Write to topic",
	EventConsume:         "Read from topic",
	EventRequestService:  "Send to service",
	EventResponseService: "Response from service",
}
