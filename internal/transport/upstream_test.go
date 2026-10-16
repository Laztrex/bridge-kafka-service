package transport_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

func TestNewUpstream_SelectsByProtocol(t *testing.T) {
	up, err := transport.NewUpstream(&schemas.ConsumerConfig{Service: schemas.HostConfig{Name: "svc", Port: "1"}})
	require.NoError(t, err, "no protocol means http")
	assert.NotNil(t, up)

	_, err = transport.NewUpstream(&schemas.ConsumerConfig{Service: schemas.HostConfig{Protocol: "soap"}})
	assert.ErrorContains(t, err, `unsupported upstream protocol "soap"`)
}
