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

// producerTuning loads content as the producer tuning file.
func producerTuning(t *testing.T, content string) *Tuning {
	t.Helper()
	withProducerConfig(t, content)
	tuning, err := LoadProducerTuning()
	require.NoError(t, err)
	return tuning
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

func TestNewProducer_AppliesDefaultsAndYAMLOverlay(t *testing.T) {
	tuning := producerTuning(t, "producer:\n  requiredacks: -1\n  retry:\n    max: 7\n")
	got := captureProducer(t)

	p, err := NewProducer(&schemas.KafkaInfo{Brokers: []string{"b:1"}, Version: "2.3.0"}, tuning)

	require.NoError(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, sarama.CompressionZSTD, got.Producer.Compression)
	assert.Equal(t, sarama.WaitForAll, got.Producer.RequiredAcks)
	assert.Equal(t, 7, got.Producer.Retry.Max)
	assert.Equal(t, sarama.V2_3_0_0, got.Version)
	assert.False(t, got.Net.TLS.Enable)
}

func TestNewProducer_TLSAndVersionFallback(t *testing.T) {
	tuning := producerTuning(t, "net:\n  tls:\n    enable: true\n")
	got := captureProducer(t)

	_, err := NewProducer(&schemas.KafkaInfo{Version: "garbage"}, tuning)

	require.NoError(t, err)
	assert.True(t, got.Net.TLS.Enable)
	assert.Equal(t, sarama.V2_7_0_0, got.Version)
}

func TestNewProducer_RealConstructorFailsAgainstDeadBroker(t *testing.T) {
	tuning := producerTuning(t, "metadata:\n  retry:\n    max: 0\n")
	_, err := NewProducer(&schemas.KafkaInfo{Brokers: []string{"127.0.0.1:1"}, Version: "2.3.0"}, tuning)
	assert.Error(t, err)
}
