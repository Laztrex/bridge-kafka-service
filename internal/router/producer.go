package router

import (
	"github.com/IBM/sarama"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// Seams overridden only by tests: a real producer needs a live broker.
var (
	newSyncProducer    = sarama.NewSyncProducer
	producerConfigPath = consts.ConfPathProducer
)

// NewProducer constructs a sarama.SyncProducer using the supplied KafkaInfo
// configuration merged with the producer tuning loaded at startup.
func NewProducer(conf *schemas.KafkaInfo, tuning *Tuning) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(conf.Version)
	if err != nil {
		version = sarama.V2_7_0_0
	}

	cfg.Version = version
	cfg.Producer.Compression = sarama.CompressionZSTD
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	if err := tuning.apply(cfg); err != nil {
		return nil, err
	}

	if cfg.Net.TLS.Enable {
		cfg.Net.TLS.Config = conf.TLSConf
	}

	return newSyncProducer(conf.Brokers, cfg)
}
