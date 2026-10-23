package app

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// runtimeState owns the application's probe, pipeline and shutdown state. Connect
// controls its lifecycle; HTTP handlers read it for readiness and detailed status.
// Readiness checks client initialization and draining, not live broker reachability.
// Pipeline retries and process-wide pause are reported by /status independently.
type runtimeState struct {
	registry       *router.ProducerRegistry
	needsProducer  bool
	consumersTotal int
	consumersUp    atomic.Int32
	pipelines      []pipeline
	pause          *router.Pause

	// draining is set when shutdown starts: /readyz answers 503 with the reason "shutting
	// down" and the publish API answers 503, while /healthz, /health and /status keep
	// answering until the HTTP server stops.
	draining atomic.Bool
	// inFlightPublish tracks publish requests being served, so the drain can wait for them.
	inFlightPublish sync.WaitGroup
}

// pipeline is one active stanza and its runtime state.
type pipeline struct {
	conf  *schemas.ConsumerConfig
	state *transport.PipelineState
}

func newRuntimeState(registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo) *runtimeState {
	active := activeConsumers(kafkaConfig)
	pipelines := make([]pipeline, len(active))
	for i, c := range active {
		pipelines[i] = pipeline{conf: c, state: transport.NewPipelineState()}
	}
	return &runtimeState{
		registry:       registry,
		needsProducer:  len(kafkaConfig.Producers) > 0,
		consumersTotal: len(active),
		pipelines:      pipelines,
		pause:          router.NewPause(),
	}
}

// consumerUp records that one more consumer client is initialized and subscribing.
func (r *runtimeState) consumerUp() { r.consumersUp.Add(1) }

// beginDrain marks the service as shutting down (design D4): /readyz answers 503 with the
// reason "shutting down" and new publish requests answer 503, while the liveness probes and
// /status keep answering until the HTTP server stops.
func (r *runtimeState) beginDrain() { r.draining.Store(true) }

// reasons lists why the service is not ready; empty means ready. While draining, "shutting
// down" comes first and /readyz answers 503 whatever the other reasons say.
func (r *runtimeState) reasons() []string {
	var out []string
	if r.draining.Load() {
		out = append(out, "shutting down")
	}
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

func (r *runtimeState) serveReadyz(w http.ResponseWriter, _ *http.Request) {
	resp, code := readyResponse{Status: "ready"}, http.StatusOK
	if reasons := r.reasons(); len(reasons) > 0 {
		resp, code = readyResponse{Status: "not ready", Reasons: reasons}, http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}

func (r *runtimeState) serveStatus(w http.ResponseWriter, _ *http.Request) {
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
