package app

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// readiness tracks what /readyz reports: the shared producer is connected (when producers are
// configured) and the consumer client of every active stanza (one with topics) is up. Liveness (/healthz, /health) does not depend on it:
// an unreachable Kafka must not make the orchestrator restart a healthy process.
// It also owns the runtime state of every active pipeline, in configuration order, and the
// process-wide pause state, which /status reports and readiness ignores.
type readiness struct {
	registry       *router.ProducerRegistry
	needsProducer  bool
	consumersTotal int
	consumersUp    atomic.Int32
	pipelines      []pipeline
	pause          *router.Pause
}

// pipeline is one active stanza and its runtime state.
type pipeline struct {
	conf  *schemas.ConsumerConfig
	state *transport.PipelineState
}

func newReadiness(registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo) *readiness {
	active := activeConsumers(kafkaConfig)
	pipelines := make([]pipeline, len(active))
	for i, c := range active {
		pipelines[i] = pipeline{conf: c, state: transport.NewPipelineState()}
	}
	return &readiness{
		registry:       registry,
		needsProducer:  len(kafkaConfig.Producers) > 0,
		consumersTotal: len(active),
		pipelines:      pipelines,
		pause:          router.NewPause(),
	}
}

// consumerUp records that one more consumer client is initialized and subscribing.
func (r *readiness) consumerUp() { r.consumersUp.Add(1) }

// reasons lists why the service is not ready; empty means ready.
func (r *readiness) reasons() []string {
	var out []string
	if r.needsProducer && !r.registry.Ready() {
		out = append(out, "kafka producer is not connected")
	}
	if int(r.consumersUp.Load()) < r.consumersTotal {
		out = append(out, "not all kafka consumers are running")
	}
	return out
}

type readyResponse struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
}

type statusResponse struct {
	Ready          bool             `json:"ready"`
	Reasons        []string         `json:"reasons,omitempty"`
	ProducerReady  bool             `json:"producer_ready"`
	ConsumersUp    int              `json:"consumers_up"`
	ConsumersTotal int              `json:"consumers_total"`
	Pipelines      []pipelineStatus `json:"pipelines"`
}

// pipelineStatus is one entry of /status pipelines: the stanza identity plus its runtime state.
type pipelineStatus struct {
	Group                string   `json:"group"`
	Topics               []string `json:"topics"`
	RetryingCalls        int      `json:"retrying_calls"`
	RetryingPublications int      `json:"retrying_publications"`
	OldestRetrySeconds   int64    `json:"oldest_retry_seconds"`
	Uncommitted          int64    `json:"uncommitted"`
	Paused               bool     `json:"paused"`
}

func (r *readiness) serveReadyz(w http.ResponseWriter, _ *http.Request) {
	resp, code := readyResponse{Status: "ready"}, http.StatusOK
	if reasons := r.reasons(); len(reasons) > 0 {
		resp, code = readyResponse{Status: "not ready", Reasons: reasons}, http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}

func (r *readiness) serveStatus(w http.ResponseWriter, _ *http.Request) {
	reasons := r.reasons()
	pipelines := make([]pipelineStatus, len(r.pipelines))
	for i, p := range r.pipelines {
		snap := p.state.Snapshot()
		pipelines[i] = pipelineStatus{
			Group:                p.conf.Group,
			Topics:               p.conf.Topics,
			RetryingCalls:        snap.RetryingCalls,
			RetryingPublications: snap.RetryingPublications,
			OldestRetrySeconds:   int64(snap.OldestRetry.Seconds()),
			Uncommitted:          snap.Uncommitted,
			Paused:               r.pause.Paused(),
		}
	}
	writeJSON(w, http.StatusOK, statusResponse{
		Ready:          len(reasons) == 0,
		Reasons:        reasons,
		ProducerReady:  r.registry.Ready(),
		ConsumersUp:    int(r.consumersUp.Load()),
		ConsumersTotal: r.consumersTotal,
		Pipelines:      pipelines,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Err(err).Msg("cannot write a probe response")
	}
}
