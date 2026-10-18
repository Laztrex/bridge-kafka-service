package transport

import "bridge-kafka-service/internal/schemas"

// PublishQueueSize is the capacity of one pipeline's response-publication queue.
const PublishQueueSize = 1000

// MaxPublishersPerPipeline caps the concurrent response writes of one pipeline: enough for the
// shared sync producer to batch several records into one produce request, few enough to keep
// goroutines and in-flight records bounded per pipeline.
const MaxPublishersPerPipeline = 16

// NewResponseQueue returns an empty response-publication queue for one pipeline. Each pipeline
// has its own, so a response topic that is slow or rejects writes holds back only its pipeline.
func NewResponseQueue() chan *schemas.ConsumerEvent {
	return make(chan *schemas.ConsumerEvent, PublishQueueSize)
}

// PublisherCount is how many responses of the pipeline are written at once: its upstream
// concurrency limit, capped at MaxPublishersPerPipeline. A max_conn: 1 pipeline gets one
// publisher, which keeps its responses in the order its upstream calls complete.
func PublisherCount(conf *schemas.ConsumerConfig) int {
	return min(concurrencyLimit(conf), MaxPublishersPerPipeline)
}
