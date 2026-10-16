package router

import (
	"sync"

	"github.com/IBM/sarama"
)

// ProducerRegistry maps allowed publish topics to a producer. It is filled in
// the background once Kafka becomes reachable, so every access is synchronised:
// HTTP handlers and the response publisher read it while startup still runs.
type ProducerRegistry struct {
	mu        sync.RWMutex
	producers map[string]sarama.SyncProducer
}

// NewProducerRegistry returns an empty (not yet ready) registry.
func NewProducerRegistry() *ProducerRegistry {
	return &ProducerRegistry{producers: map[string]sarama.SyncProducer{}}
}

// Set binds the producer to every given topic; the topic list is the allow-list.
func (r *ProducerRegistry) Set(producer sarama.SyncProducer, topics []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range topics {
		r.producers[t] = producer
	}
}

// Lookup returns the producer registered for topic.
func (r *ProducerRegistry) Lookup(topic string) (sarama.SyncProducer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.producers[topic]
	return p, ok
}

// Ready reports whether at least one topic has a producer.
func (r *ProducerRegistry) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.producers) > 0
}

// Producers returns the registered producers, one entry per topic binding.
func (r *ProducerRegistry) Producers() []sarama.SyncProducer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]sarama.SyncProducer, 0, len(r.producers))
	for _, p := range r.producers {
		out = append(out, p)
	}
	return out
}
