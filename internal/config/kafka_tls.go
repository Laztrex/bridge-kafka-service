package config

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
)

// NewKafkaTLS builds the TLS settings for the Kafka brokers from the kafka.tls.* keys. Missing
// paths fall back to the files under certs/. As before, any problem with the material degrades
// to an insecure connection with a warning instead of stopping startup.
func NewKafkaTLS(t KafkaTLS) *tls.Config {
	pathCert, pathKey, pathCaCert, skipVerify := t.CertPath, t.KeyPath, t.CAPath, t.SkipVerify

	if pathCert == "" {
		pathCert = consts.PathCertDefault
	}
	if pathKey == "" {
		pathKey = consts.PathKeyDefault
	}
	if pathCaCert == "" {
		pathCaCert = consts.PathCaCertDefault
	}

	if _, err := os.Stat(pathCert); os.IsNotExist(err) {
		log.Warn().Msg("TLS certificate not found, using insecure connection")
		return &tls.Config{InsecureSkipVerify: true}
	}
	if _, err := os.Stat(pathKey); os.IsNotExist(err) {
		log.Warn().Msg("TLS key not found, using insecure connection")
		return &tls.Config{InsecureSkipVerify: true}
	}

	cert, err := tls.LoadX509KeyPair(pathCert, pathKey)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to load TLS certificate, using insecure connection")
		return &tls.Config{InsecureSkipVerify: true}
	}

	caCert, err := os.ReadFile(pathCaCert)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to load CA certificate, using insecure connection")
		return &tls.Config{InsecureSkipVerify: true}
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Warn().Msg("Failed to parse CA certificate, using insecure connection")
		return &tls.Config{InsecureSkipVerify: true}
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: skipVerify,
	}
}
