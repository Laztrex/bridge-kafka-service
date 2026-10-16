package app

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
)

// readiness tracks what /readyz reports: the shared producer is connected (when producers are
// configured) and every consumer client is up. Liveness (/healthz, /health) does not depend on it:
// an unreachable Kafka must not make the orchestrator restart a healthy process.
type readiness struct {
	registry       *router.ProducerRegistry
	needsProducer  bool
	consumersTotal int
	consumersUp    atomic.Int32
}

func newReadiness(registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo) *readiness {
	return &readiness{
		registry:       registry,
		needsProducer:  len(kafkaConfig.Producers) > 0,
		consumersTotal: len(kafkaConfig.Consumers),
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
	Ready          bool     `json:"ready"`
	Reasons        []string `json:"reasons,omitempty"`
	ProducerReady  bool     `json:"producer_ready"`
	ConsumersUp    int      `json:"consumers_up"`
	ConsumersTotal int      `json:"consumers_total"`
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
	writeJSON(w, http.StatusOK, statusResponse{
		Ready:          len(reasons) == 0,
		Reasons:        reasons,
		ProducerReady:  r.registry.Ready(),
		ConsumersUp:    int(r.consumersUp.Load()),
		ConsumersTotal: r.consumersTotal,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Err(err).Msg("cannot write a probe response")
	}
}
