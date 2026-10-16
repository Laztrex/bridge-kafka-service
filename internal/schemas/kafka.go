package schemas

import (
	"crypto/tls"
	"strings"
	"time"
)

// Upstream protocols (service.protocol) and TLS modes (service.tls.mode).
const (
	ProtocolHTTP = "http"
	ProtocolGRPC = "grpc"

	TLSModeNone = "none"
	TLSModeTLS  = "tls"
	TLSModeMTLS = "mtls"
)

// Upstream failure policies (on_failure).
const (
	FailureSkip  = "skip"
	FailureRetry = "retry"
)

type KafkaInfo struct {
	Brokers []string `mapstructure:"brokers"`
	Version string   `mapstructure:"version"`
	TLSConf *tls.Config

	Consumers []*ConsumerConfig `mapstructure:"consumer"`
	Producers []*ProducerConfig `mapstructure:"producer"`
}

type ConsumerConfig struct {
	Topics        []string   `mapstructure:"topics"`
	Group         string     `mapstructure:"group"`
	ClientId      string     `mapstructure:"client_id"`
	Service       HostConfig `mapstructure:"service"`
	ResponseTopic string     `mapstructure:"response_topic"`
	OnFailure     string     `mapstructure:"on_failure"` // skip (default) | retry

	// UpstreamTLS is built at startup from Service.TLS (nil for plaintext).
	UpstreamTLS *tls.Config `mapstructure:"-"`
	// DrainWindow is how long a stopping pipeline waits for in-flight upstream calls before
	// abandoning them (timeouts.shutdown; 0 means the default).
	DrainWindow time.Duration `mapstructure:"-"`
}

type ProducerConfig struct {
	Topic string `mapstructure:"topic"`
}

type HostConfig struct {
	Name     string        `mapstructure:"name"`
	Port     string        `mapstructure:"port"`
	Path     string        `mapstructure:"path"`
	MaxConn  int           `mapstructure:"max_conn,default=-1"`
	Timeout  time.Duration `mapstructure:"timeout"`
	Protocol string        `mapstructure:"protocol"` // http (default) | grpc
	TLS      ServiceTLS    `mapstructure:"tls"`
}

// ServiceTLS configures encryption of the connection to the upstream service.
type ServiceTLS struct {
	Mode       string `mapstructure:"mode"` // none (default) | tls | mtls
	CAPath     string `mapstructure:"ca_path"`
	CertPath   string `mapstructure:"cert_path"`
	KeyPath    string `mapstructure:"key_path"`
	ServerName string `mapstructure:"server_name"`
	SkipVerify bool   `mapstructure:"skip_verify"`
}

// EffectiveProtocol returns the configured protocol, defaulting to http.
func (h *HostConfig) EffectiveProtocol() string {
	if p := strings.ToLower(strings.TrimSpace(h.Protocol)); p != "" {
		return p
	}
	return ProtocolHTTP
}

// EffectiveFailurePolicy returns the configured failure policy, defaulting to skip.
func (c *ConsumerConfig) EffectiveFailurePolicy() string {
	if p := strings.ToLower(strings.TrimSpace(c.OnFailure)); p != "" {
		return p
	}
	return FailureSkip
}

// EffectiveMode returns the configured TLS mode, defaulting to none (plaintext).
func (t *ServiceTLS) EffectiveMode() string {
	if m := strings.ToLower(strings.TrimSpace(t.Mode)); m != "" {
		return m
	}
	return TLSModeNone
}

// Encrypted reports whether the upstream connection must use TLS.
func (t *ServiceTLS) Encrypted() bool { return t.EffectiveMode() != TLSModeNone }

// URI is the HTTP address of the service; the scheme follows the TLS mode.
func (h *HostConfig) URI() string {
	scheme := "http"
	if h.TLS.Encrypted() {
		scheme = "https"
	}
	return scheme + "://" + h.Name + ":" + h.Port + h.Path
}

// Target is the host:port address used by non-HTTP protocols (gRPC).
func (h *HostConfig) Target() string {
	return h.Name + ":" + h.Port
}
