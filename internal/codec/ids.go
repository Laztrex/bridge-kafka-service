package codec

import (
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
)

// systemID is the x-client-id put on records that carry none (SYSTEM_ID, set once at startup).
var systemID atomic.Value

// SetSystemID sets the x-client-id used for records that carry none.
func SetSystemID(id string) { systemID.Store(id) }

// SystemID returns the x-client-id for a record that carries none.
func SystemID() string {
	id, _ := systemID.Load().(string)
	log.Warn().Msgf("'%s' not received. Set from env: %s", consts.HeaderClientId, id)
	return id
}

// NewRequestID returns a new x-request-id for a request or record that carries none.
func NewRequestID() string {
	id := uuid.New().String()
	log.Warn().Msgf("'%s' not received. Set new: %s", consts.HeaderRequestIdKey, id)
	return id
}
