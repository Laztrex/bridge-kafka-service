package router

import (
	"bytes"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func captureWarnings(t *testing.T) *logBuffer {
	t.Helper()
	buf := &logBuffer{}
	old := log.Logger
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = old })
	return buf
}

func useConsumerFile(t *testing.T, content string) {
	t.Helper()
	old := consumerConfigPath
	consumerConfigPath = writeYAML(t, content)
	t.Cleanup(func() { consumerConfigPath = old })
}

func applied(t *testing.T, tuning *Tuning) *sarama.Config {
	t.Helper()
	cfg := sarama.NewConfig()
	require.NoError(t, tuning.apply(cfg))
	return cfg
}

func TestLoadTuning_UnknownKeysAreWarnedAndIgnored(t *testing.T) {
	logs := captureWarnings(t)
	withProducerConfig(t, "net:\n  maxopenrequest: 1\n  MaxOpenRequests: 2\nproducer:\n  retry:\n    max: 7\n")

	tuning, err := LoadProducerTuning()

	require.NoError(t, err)
	cfg := applied(t, tuning)
	assert.Equal(t, sarama.NewConfig().Net.MaxOpenRequests, cfg.Net.MaxOpenRequests, "misspelled keys keep the sarama default")
	assert.Equal(t, 7, cfg.Producer.Retry.Max, "valid keys are still applied")
	assert.Contains(t, logs.String(), "unknown tuning key net.maxopenrequest in "+producerConfigPath)
	assert.Contains(t, logs.String(), "unknown tuning key net.MaxOpenRequests in ")
}

func TestLoadTuning_UnsettableKeysAreWarnedInsteadOfPanicking(t *testing.T) {
	logs := captureWarnings(t)
	withProducerConfig(t, "producer:\n  partitioner: random\n  interceptors: [x]\nnet:\n  sasl:\n    tokenprovider: x\n")

	tuning, err := LoadProducerTuning()

	require.NoError(t, err)
	assert.NotPanics(t, func() { applied(t, tuning) })
	for _, key := range []string{"producer.partitioner", "producer.interceptors", "net.sasl.tokenprovider"} {
		assert.Contains(t, logs.String(), "tuning key "+key+" in "+producerConfigPath+" cannot be set from YAML and is ignored")
	}

	useConsumerFile(t, "consumer:\n  group:\n    rebalance:\n      groupstrategies: [sticky]\n")
	tuning, err = LoadConsumerTuning()
	require.NoError(t, err)
	assert.Equal(t, sarama.NewConfig().Consumer.Group.Rebalance.GroupStrategies[0].Name(),
		applied(t, tuning).Consumer.Group.Rebalance.GroupStrategies[0].Name())
	assert.Contains(t, logs.String(), "tuning key consumer.group.rebalance.groupstrategies")
}

func TestLoadConsumerTuning_RebalanceStrategy(t *testing.T) {
	for value, want := range map[string]string{
		"range": "range", "roundrobin": "roundrobin", "sticky": "sticky",
		"cooperative-sticky": "cooperative-sticky", " Sticky ": "sticky",
	} {
		t.Run(value, func(t *testing.T) {
			logs := captureWarnings(t)
			useConsumerFile(t, "consumer:\n  group:\n    rebalance:\n      strategy: \""+value+"\"\n")

			tuning, err := LoadConsumerTuning()

			require.NoError(t, err)
			strategies := applied(t, tuning).Consumer.Group.Rebalance.GroupStrategies
			require.Len(t, strategies, 1)
			assert.Equal(t, want, strategies[0].Name())
			assert.Empty(t, logs.String(), "the strategy key is the application's own, not a warning")
		})
	}

	t.Run("unknown", func(t *testing.T) {
		useConsumerFile(t, "consumer:\n  group:\n    rebalance:\n      strategy: fair\n")
		_, err := LoadConsumerTuning()
		require.Error(t, err)
		for _, want := range []string{"consumer.group.rebalance.strategy", `"fair"`, "range, roundrobin, sticky, cooperative-sticky"} {
			assert.ErrorContains(t, err, want)
		}
	})

	t.Run("absent keeps the default", func(t *testing.T) {
		useConsumerFile(t, "consumer:\n  return:\n    errors: true\n")
		tuning, err := LoadConsumerTuning()
		require.NoError(t, err)
		cfg := sarama.NewConfig()
		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
		require.NoError(t, tuning.apply(cfg))
		assert.Equal(t, "range", cfg.Consumer.Group.Rebalance.GroupStrategies[0].Name())
	})
}

func TestLoadConsumerTuning_AutoCommitIntervalIsAnApplicationKey(t *testing.T) {
	logs := captureWarnings(t)
	useConsumerFile(t, "OffsetsAutoCommitInterval: 2s\nconsumer:\n  return:\n    errors: true\n")

	tuning, err := LoadConsumerTuning()

	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, applied(t, tuning).Consumer.Offsets.AutoCommit.Interval)
	assert.Empty(t, logs.String())
}

func TestLoadTuning_FileErrorsNameTheFile(t *testing.T) {
	old := consumerConfigPath
	consumerConfigPath = filepath.Join(t.TempDir(), "nope.yaml")
	t.Cleanup(func() { consumerConfigPath = old })
	_, err := LoadConsumerTuning()
	assert.ErrorContains(t, err, "unable to locate configuration file "+consumerConfigPath)

	withProducerConfig(t, "net: [unclosed")
	_, err = LoadProducerTuning()
	assert.ErrorContains(t, err, "unable to parse configuration file "+producerConfigPath)

	withProducerConfig(t, "producer:\n  retry:\n    max: many\n")
	_, err = LoadProducerTuning()
	assert.ErrorContains(t, err, producerConfigPath, "a value of the wrong type is reported at load time")

	withProducerConfig(t, "- a\n- b\n")
	_, err = LoadProducerTuning()
	assert.ErrorContains(t, err, "top level must be a mapping")
}

func TestLoadTuning_EmptyFileAndNilTuningApplyNothing(t *testing.T) {
	withProducerConfig(t, "")
	tuning, err := LoadProducerTuning()
	require.NoError(t, err)
	assert.Equal(t, sarama.NewConfig().Producer.Retry.Max, applied(t, tuning).Producer.Retry.Max)

	var none *Tuning
	assert.NoError(t, none.apply(sarama.NewConfig()))
}

// The tuning files shipped in configs/ must load without a single warning.
func TestShippedTuningFilesLoadWithoutWarnings(t *testing.T) {
	logs := captureWarnings(t)
	t.Chdir("../..")

	_, err := LoadConsumerTuning()
	require.NoError(t, err)
	_, err = LoadProducerTuning()
	require.NoError(t, err)

	assert.False(t, strings.Contains(logs.String(), "tuning key"), "unexpected warnings: %s", logs.String())
}
