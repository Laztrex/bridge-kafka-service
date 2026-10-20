package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/schemas"
)

// PrepareUpstreams validates the upstream settings of every consumer stanza
// (protocol, target address, failure policy, TLS mode and material) and builds the TLS configuration once.
// on_failure: retry requires max_conn > 0, so retries become backpressure. Every problem of every
// stanza is reported, joined into one error, so that startup fails instead of silently running
// with weaker security than requested or with an address no call can use.
func PrepareUpstreams(config *schemas.KafkaInfo) error {
	var errs []error
	for i, c := range config.Consumers {
		if c == nil {
			continue
		}
		stanza := fmt.Sprintf("consumer #%d (topics %v)", i+1, c.Topics)

		switch p := c.Service.EffectiveProtocol(); p {
		case schemas.ProtocolHTTP, schemas.ProtocolGRPC:
			if len(c.Topics) > 0 {
				errs = append(errs, validateTarget(stanza, p, c.Service)...)
			}
		default:
			errs = append(errs, fmt.Errorf("%s: unknown service.protocol %q (allowed: %s, %s)",
				stanza, p, schemas.ProtocolHTTP, schemas.ProtocolGRPC))
		}

		switch p := c.EffectiveFailurePolicy(); p {
		case schemas.FailureSkip:
		case schemas.FailureRetry:
			if c.Service.MaxConn <= 0 {
				errs = append(errs, fmt.Errorf("%s: on_failure %q requires service.max_conn > 0", stanza, p))
			}
			if c.ResponseTopic != "" && !isProducerTopic(config.Producers, c.ResponseTopic) {
				errs = append(errs, fmt.Errorf("%s: on_failure %q requires response_topic %q to be listed as a producer topic", stanza, p, c.ResponseTopic))
			}
		default:
			errs = append(errs, fmt.Errorf("%s: unknown on_failure %q (allowed: %s, %s)",
				stanza, p, schemas.FailureSkip, schemas.FailureRetry))
		}

		tlsConfig, err := NewUpstreamTLS(stanza, c.Service.TLS)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		c.UpstreamTLS = tlsConfig
	}
	return errors.Join(errs...)
}

// validateTarget checks that an active stanza's service address is usable by its protocol, so no
// call can fail on the address itself: service.name without whitespace or control characters and
// service.port from 1 to 65535 for both protocols; for http also a service.path starting with "/"
// and a URL that forms a request. Nothing is resolved: an unreachable host stays a transport
// failure at call time.
func validateTarget(stanza, protocol string, svc schemas.HostConfig) []error {
	var errs []error
	if svc.Name == "" || strings.IndexFunc(svc.Name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		errs = append(errs, fmt.Errorf("%s: service.name %q is not a valid host for %s", stanza, svc.Name, protocol))
	}
	if port, err := strconv.Atoi(svc.Port); err != nil || port < 1 || port > 65535 || strconv.Itoa(port) != svc.Port {
		errs = append(errs, fmt.Errorf("%s: service.port %q is not a valid port for %s", stanza, svc.Port, protocol))
	}
	if protocol != schemas.ProtocolHTTP || len(errs) > 0 {
		return errs
	}
	if !strings.HasPrefix(svc.Path, "/") {
		return append(errs, fmt.Errorf("%s: service.path %q must start with \"/\"", stanza, svc.Path))
	}
	if _, err := http.NewRequest(http.MethodPost, svc.URI(), nil); err != nil {
		errs = append(errs, fmt.Errorf("%s: service address %q does not form an HTTP request: %w", stanza, svc.URI(), err))
	}
	return errs
}

// isProducerTopic reports whether topic is served by one of the configured producers.
func isProducerTopic(producers []*schemas.ProducerConfig, topic string) bool {
	for _, p := range producers {
		if p != nil && p.Topic == topic {
			return true
		}
	}
	return false
}

// NewUpstreamTLS builds the client TLS configuration for an upstream service. Mode "none" yields
// nil (plaintext); without a CA path the system roots are used. Unlike the Kafka broker settings
// it never falls back to insecure: every problem is an error naming the stanza and the file.
func NewUpstreamTLS(stanza string, c schemas.ServiceTLS) (*tls.Config, error) {
	mode := c.EffectiveMode()
	switch mode {
	case schemas.TLSModeNone:
		return nil, nil
	case schemas.TLSModeTLS, schemas.TLSModeMTLS:
	default:
		return nil, fmt.Errorf("%s: unknown service.tls.mode %q (allowed: %s, %s, %s)",
			stanza, mode, schemas.TLSModeNone, schemas.TLSModeTLS, schemas.TLSModeMTLS)
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.SkipVerify,
	}
	if c.SkipVerify {
		log.Warn().Msgf("%s: service.tls.skip_verify is enabled, the upstream certificate is NOT verified", stanza)
	}

	if c.CAPath != "" {
		pem, err := os.ReadFile(c.CAPath)
		if err != nil {
			return nil, fmt.Errorf("%s: cannot read service.tls.ca_path %q: %w", stanza, c.CAPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: service.tls.ca_path %q contains no valid PEM certificate", stanza, c.CAPath)
		}
		cfg.RootCAs = pool
	}

	if mode == schemas.TLSModeMTLS {
		if c.CertPath == "" || c.KeyPath == "" {
			missing := "cert_path"
			if c.CertPath != "" {
				missing = "key_path"
			}
			return nil, fmt.Errorf("%s: service.tls.mode %q requires service.tls.cert_path and key_path (missing %s)",
				stanza, mode, missing)
		}
		pair, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("%s: cannot load client certificate (cert_path %q, key_path %q): %w",
				stanza, c.CertPath, c.KeyPath, err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}
