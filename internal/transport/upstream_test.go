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

func TestConcurrencyLimit(t *testing.T) {
	assert.Equal(t, 512, transport.ConcurrencyLimit(&schemas.ConsumerConfig{}),
		"max_conn 0 means fast mode with the built-in ceiling")
	assert.Equal(t, 512, transport.ConcurrencyLimit(&schemas.ConsumerConfig{
		Service: schemas.HostConfig{MaxConn: -1},
	}), "a negative max_conn also selects fast mode")
	assert.Equal(t, 8, transport.ConcurrencyLimit(&schemas.ConsumerConfig{
		Service: schemas.HostConfig{MaxConn: 8},
	}), "a positive max_conn is the limit itself")
}
