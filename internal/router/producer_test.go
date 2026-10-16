package router

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/mocksarama"
)

func withProducerConfig(t *testing.T, content string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "producer.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	old := producerConfigPath
	producerConfigPath = p
	t.Cleanup(func() { producerConfigPath = old })
}

func captureProducer(t *testing.T) *sarama.Config {
	t.Helper()
	var got sarama.Config
	old := newSyncProducer
	newSyncProducer = func(_ []string, cfg *sarama.Config) (sarama.SyncProducer, error) {
		got = *cfg
		return mocksarama.NewSyncProducer(t), nil
	}
	t.Cleanup(func() { newSyncProducer = old })
	return &got
}

func TestNewProducer_MissingAndBrokenConfig(t *testing.T) {
	old := producerConfigPath
	producerConfigPath = filepath.Join(t.TempDir(), "nope.yaml")
	t.Cleanup(func() { producerConfigPath = old })
	_, err := NewProducer(&schemas.KafkaInfo{})
	assert.ErrorContains(t, err, "unable to locate configuration file")

	withProducerConfig(t, "net: [unclosed")
	_, err = NewProducer(&schemas.KafkaInfo{})
	assert.ErrorContains(t, err, "unable to parse configuration")
}

func TestNewProducer_AppliesDefaultsAndYAMLOverlay(t *testing.T) {
	withProducerConfig(t, "producer:\n  requiredacks: -1\n  retry:\n    max: 7\n")
	got := captureProducer(t)

	p, err := NewProducer(&schemas.KafkaInfo{Brokers: []string{"b:1"}, Version: "2.3.0"})

	require.NoError(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, sarama.CompressionZSTD, got.Producer.Compression)
	assert.Equal(t, sarama.WaitForAll, got.Producer.RequiredAcks)
	assert.Equal(t, 7, got.Producer.Retry.Max)
	assert.Equal(t, sarama.V2_3_0_0, got.Version)
	assert.False(t, got.Net.TLS.Enable)
}

func TestNewProducer_TLSAndVersionFallback(t *testing.T) {
	withProducerConfig(t, "net:\n  tls:\n    enable: true\n")
	got := captureProducer(t)

	_, err := NewProducer(&schemas.KafkaInfo{Version: "garbage"})

	require.NoError(t, err)
	assert.True(t, got.Net.TLS.Enable)
	assert.Equal(t, sarama.V2_7_0_0, got.Version)
}

func TestNewProducer_RealConstructorFailsAgainstDeadBroker(t *testing.T) {
	withProducerConfig(t, "metadata:\n  retry:\n    max: 0\n")
	_, err := NewProducer(&schemas.KafkaInfo{Brokers: []string{"127.0.0.1:1"}, Version: "2.3.0"})
	assert.Error(t, err)
}
