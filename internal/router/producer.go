package router

import (
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"gopkg.in/yaml.v3"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// Seams overridden only by tests: a real producer needs a live broker.
var (
	newSyncProducer    = sarama.NewSyncProducer
	producerConfigPath = consts.ConfPathProducer
)

// NewProducer constructs a sarama.SyncProducer using the supplied KafkaInfo
// configuration merged with YAML tunables loaded from consts.ConfPathProducer.
func NewProducer(conf *schemas.KafkaInfo) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(conf.Version)
	if err != nil {
		version = sarama.V2_7_0_0
	}

	cfg.Version = version
	cfg.Producer.Compression = sarama.CompressionZSTD
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	raw, err := os.ReadFile(producerConfigPath)
	if err != nil {
		return nil, fmt.Errorf("unable to locate configuration file: %w", err)
	}

	err = yaml.Unmarshal(raw, &cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse configuration: %w", err)
	}

	if cfg.Net.TLS.Enable {
		cfg.Net.TLS.Config = conf.TLSConf
	}

	return newSyncProducer(conf.Brokers, cfg)
}
