package config

import (
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/schemas"
)

// LogKafkaConfig logs the effective Kafka setup at startup as structured records: one for the
// cluster, one per consumer and producer. It holds no secrets (TLS material is reported only
// as enabled or not) and never panics, so a broken config cannot stop startup here.
func LogKafkaConfig(config *schemas.KafkaInfo) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Msgf("kafka config cannot be logged: %v", r)
		}
	}()
	if config == nil {
		log.Warn().Msg("kafka config is not loaded")
		return
	}

	log.Info().
		Strs("brokers", config.Brokers).
		Str("kafka_version", config.Version).
		Bool("tls", config.TLSConf != nil).
		Int("consumers", len(config.Consumers)).
		Int("producers", len(config.Producers)).
		Msg("kafka config")

	for i, c := range config.Consumers {
		if c == nil {
			continue
		}
		service := ""
		if c.Service.Name != "" && c.Service.Port != "" {
			service = c.Service.URI()
		}
		log.Info().
			Int("consumer", i+1).
			Str("client_id", c.ClientId).
			Str("group", c.Group).
			Strs("topics", c.Topics).
			Str("service", service).
			Str("protocol", c.Service.EffectiveProtocol()).
			Str("response_topic", c.ResponseTopic).
			Msg("consumer config")
	}
	for i, p := range config.Producers {
		if p == nil {
			continue
		}
		log.Info().Int("producer", i+1).Str("topic", p.Topic).Msg("producer config")
	}
}
