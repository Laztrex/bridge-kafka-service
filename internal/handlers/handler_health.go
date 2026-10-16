package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/schemas"
)

// Health is the liveness probe: it answers 200 while the process runs.
// @Summary Liveness probe
// @Produce json
// @Success 200 {object} schemas.Health
// @Router /health [get]
func Health(w http.ResponseWriter, _ *http.Request) {
	ans := schemas.Health{Status: "running"}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(ans); err != nil {
		log.Err(err).Msg("can't send a reply in Health method")
	}
}
