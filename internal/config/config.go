// Package config loads the application configuration (configs/config_app.yaml) without global
// state. It keeps the file format, key spellings and environment overrides of the former
// viper-based loader (see source), so existing deployments need no changes.
//
// The Kafka client tunables (configs/config_consumer.yaml, configs/config_producer.yaml) are a
// separate concern: the router reads them straight into sarama.Config.
package config

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

const (
	// DefaultPath is the config path when neither the -config flag nor CONFIG_PATH is set.
	DefaultPath = consts.ConfPathConn
	// PathEnv is the environment variable (also read from the .env file) holding the config path.
	PathEnv = "CONFIG_PATH"
	// EnvFile is the optional dotenv file in the working directory; only CONFIG_PATH is taken from it.
	EnvFile = consts.EnvFile

	DefaultLogLevel        = "debug"
	DefaultShutdownTimeout = 10 * time.Second
)

// DefaultHTTPAddr is the address of the HTTP server (publish API and probes) when http.addr is unset.
var DefaultHTTPAddr = consts.ControllerHost + ":" + consts.ControllerHTTPPort

// The yaml tags below name the config keys: they document the file format and feed make
// docs-check. Values are read by Load (see source), not by tag-based decoding.

// KafkaTLS holds the kafka.tls.* keys: client certificate, key and CA for the brokers.
type KafkaTLS struct {
	CertPath   string `yaml:"kafka.tls.client.cert.path"`
	KeyPath    string `yaml:"kafka.tls.client.key.path"`
	CAPath     string `yaml:"kafka.tls.ca.path"`
	SkipVerify bool   `yaml:"kafka.tls.skip.insecure.verify"`
}

// Retention holds the retention.* keys: how many backups of the metric and audit files are kept and
// for how many days. 0 means no limit, which keeps every backup.
type Retention struct {
	MaxBackups int `yaml:"retention.max_backups"`
	MaxAgeDays int `yaml:"retention.max_age_days"`
}

// Config is the loaded application configuration.
type Config struct {
	LogLevel          string        `yaml:"logLevel"`
	HTTPAddr          string        `yaml:"http.addr"`
	PprofAddr         string        `yaml:"pprof.addr"`          // empty disables profiling
	AuditGRPCEndpoint string        `yaml:"audit.grpc_endpoint"` // empty disables the gRPC audit sink
	ShutdownTimeout   time.Duration `yaml:"timeouts.shutdown"`
	KafkaTLS          KafkaTLS      `yaml:"-"`
	Retention         Retention     `yaml:"-"`

	// Kafka holds kafka.brokers, kafka.version and the consumer and producer sections;
	// TLSConf is built from KafkaTLS by Load.
	Kafka *schemas.KafkaInfo

	// Environment-style settings: the variable, or the same key in the file.
	MetricPath     string `yaml:"METRIC_PATH"`
	MetricFileName string `yaml:"METRIC_FILE_NAME"`
	SystemID       string `yaml:"SYSTEM_ID"` // x-client-id when a record carries none

	// Warnings lists keys that the loader does not know. They are not errors, so that files
	// written for older versions keep loading; the caller logs them.
	Warnings []string
}

// MetricFilePath is the metrics file (JSON lines); the audit file is <MetricFilePath>.audit.
func (c *Config) MetricFilePath() string {
	return filepath.Join(c.MetricPath, c.MetricFileName)
}

// ResolvePath picks the config path: the -config flag, then CONFIG_PATH from the environment,
// then CONFIG_PATH from the .env file in the working directory, then DefaultPath. usedDefault
// reports the last case, which the caller logs as a warning.
func ResolvePath(flagValue string, getenv func(string) string) (path string, usedDefault bool) {
	if flagValue != "" {
		return flagValue, false
	}
	if v := getenv(PathEnv); v != "" {
		return v, false
	}
	if v := dotenvValue(EnvFile, PathEnv); v != "" {
		return v, false
	}
	return DefaultPath, true
}

// LogFilePath is the run log file. Like the former loader, which opened it before reading the
// config, it honours only the LOG_PATH and LOG_FILE_NAME environment variables.
func LogFilePath(getenv func(string) string) string {
	dir, name := getenv("LOG_PATH"), getenv("LOG_FILE_NAME")
	if dir == "" {
		dir = consts.LogPath
	}
	if name == "" {
		name = consts.LogFile
	}
	return filepath.Join(dir, name)
}

// Load reads and parses the config at path, applies defaults and builds the Kafka TLS settings.
// Validation of the Kafka setup is separate (Validate, PrepareUpstreams).
func Load(path string, getenv func(string) string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	src, err := newSource(raw, getenv)
	if err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	cfg := &Config{
		LogLevel:          orDefault(src.String("logLevel"), DefaultLogLevel),
		HTTPAddr:          orDefault(src.String("http.addr"), DefaultHTTPAddr),
		PprofAddr:         src.String("pprof.addr"),
		AuditGRPCEndpoint: src.String("audit.grpc_endpoint"),
		ShutdownTimeout:   DefaultShutdownTimeout,
		KafkaTLS: KafkaTLS{
			CertPath:   src.String("kafka.tls.client.cert.path"),
			KeyPath:    src.String("kafka.tls.client.key.path"),
			CAPath:     src.String("kafka.tls.ca.path"),
			SkipVerify: src.Bool("kafka.tls.skip.insecure.verify"),
		},
		MetricPath:     orDefault(src.String("METRIC_PATH"), consts.MetricPath),
		MetricFileName: orDefault(src.String("METRIC_FILE_NAME"), consts.MetricFile),
		SystemID:       src.String("SYSTEM_ID"),
		Kafka:          &schemas.KafkaInfo{},
	}
	if v := src.String("timeouts.shutdown"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("timeouts.shutdown: expected a positive duration like 10s, got %q", v)
		}
		cfg.ShutdownTimeout = d
	}

	if cfg.Retention.MaxBackups, err = wholeNumber(src, "retention.max_backups"); err != nil {
		return nil, err
	}
	if cfg.Retention.MaxAgeDays, err = wholeNumber(src, "retention.max_age_days"); err != nil {
		return nil, err
	}

	var unused []string
	collect := func(u []string, err error) error {
		unused = append(unused, u...)
		return err
	}
	if err := collect(src.decode("kafka", cfg.Kafka)); err != nil {
		return nil, fmt.Errorf("decode kafka config: %w", err)
	}
	var consumers []*schemas.ConsumerConfig
	if err := collect(src.decode("consumer", &consumers)); err != nil {
		return nil, fmt.Errorf("decode consumers config: %w", err)
	}
	var producers []*schemas.ProducerConfig
	if err := collect(src.decode("producer", &producers)); err != nil {
		return nil, fmt.Errorf("decode producers config: %w", err)
	}
	for _, c := range consumers {
		applyConsumerDefaults(src, c)
		if c != nil {
			c.DrainWindow = cfg.ShutdownTimeout
		}
	}
	cfg.Kafka.Consumers, cfg.Kafka.Producers = consumers, producers
	cfg.Kafka.TLSConf = NewKafkaTLS(cfg.KafkaTLS)

	cfg.Warnings = unknownKeys(src, unused)
	return cfg, nil
}

// applyConsumerDefaults fills unset service settings.
func applyConsumerDefaults(src *source, c *schemas.ConsumerConfig) {
	if c == nil {
		return
	}
	if c.Service.Timeout == 0 {
		c.Service.Timeout = consts.DefaultServiceTimeout
	}
	if c.Service.Port == "" {
		c.Service.Port = servicePort(src, c.Service.Name)
	}
	if c.Service.Path == "" {
		c.Service.Path = consts.BasePathServiceDefault
	}
}

// servicePort is <HOST>_SERVICE_PORT (host upper-cased, dashes as underscores) from the
// environment or the file, otherwise the default port.
func servicePort(src *source, host string) string {
	if host == "" {
		return consts.PortServiceDefault
	}
	return orDefault(src.String(strings.ToUpper(strings.ReplaceAll(host, "-", "_"))+"_SERVICE_PORT"), consts.PortServiceDefault)
}

// knownTopLevel are the keys the loader reads at the top level (lower case).
var knownTopLevel = map[string]bool{
	"loglevel": true, "http": true, "pprof": true, "audit": true, "kafka": true,
	"consumer": true, "producer": true, "timeouts": true, "retention": true,
	"log_path": true, "log_file_name": true, "metric_path": true, "metric_file_name": true,
	"system_id": true, "config_path": true,
}

// unknownKeys lists top-level keys the loader ignores, plus unknown keys inside the decoded
// sections. Keys under kafka.tls are read separately and are known.
func unknownKeys(src *source, unused []string) []string {
	var out []string
	for k := range src.values {
		root := strings.SplitN(k, ".", 2)[0]
		if knownTopLevel[root] || strings.HasSuffix(k, "_service_port") {
			continue
		}
		out = append(out, k)
	}
	for _, u := range unused {
		if !strings.HasPrefix(u, "kafka.tls") {
			out = append(out, u)
		}
	}
	sort.Strings(out)
	return out
}

// wholeNumber reads key as a retention limit: unset is 0, otherwise a whole number >= 0.
func wholeNumber(src *source, key string) (int, error) {
	v := src.String(key)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: expected a whole number >= 0, got %q", key, v)
	}
	return n, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// dotenvValue returns key from a dotenv file (KEY=VALUE lines, optional "export ", quotes,
// # comments), case-insensitively; a missing file yields "".
func dotenvValue(path, key string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(k), key) {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		return v
	}
	return ""
}

// Validate checks that the Kafka setup can work: brokers are set, every consumer and producer entry is set,
// every active consumer has a group and a service, and there is at least one active consumer or
// producer. A non-empty kafka.version must parse; an empty one means the client default (2.7.0).
// With active consumers a version below 0.10.2 is rejected because consumer groups need it; with
// producers, one below 0.11, because every published record carries headers. Every problem is
// reported, joined into one error.
func Validate(k *schemas.KafkaInfo) error {
	var errs []error
	if len(k.Brokers) == 0 {
		errs = append(errs, errors.New("no Kafka bootstrap brokers defined"))
	}
	hasActiveConsumers := false
	for i, consumer := range k.Consumers {
		if consumer == nil {
			errs = append(errs, fmt.Errorf("consumer #%d is empty", i+1))
			continue
		}
		if len(consumer.Topics) == 0 {
			continue
		}
		hasActiveConsumers = true
		if consumer.Group == "" {
			errs = append(errs, fmt.Errorf("no Kafka consumer group defined for consumer #%d", i+1))
		}
		if consumer.Service.Name == "" {
			errs = append(errs, fmt.Errorf("no service host defined for consumer #%d", i+1))
		}
	}
	if k.Version != "" {
		v, err := sarama.ParseKafkaVersion(k.Version)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("kafka.version %q is not a Kafka version (for example 2.3.0, or 0.11.0.0 for 0.x); leave it empty for the default 2.7.0", k.Version))
		case hasActiveConsumers && !v.IsAtLeast(sarama.V0_10_2_0):
			errs = append(errs, fmt.Errorf("kafka.version %q is below 0.10.2, required by consumer groups", k.Version))
		case len(k.Producers) > 0 && !v.IsAtLeast(sarama.V0_11_0_0):
			errs = append(errs, fmt.Errorf("kafka.version %q is below 0.11, required for record headers (every published record carries them)", k.Version))
		}
	}
	hasProducer := false
	for i, producer := range k.Producers {
		if producer == nil {
			errs = append(errs, fmt.Errorf("producer #%d is empty", i+1))
			continue
		}
		if producer.Topic != "" {
			hasProducer = true
		}
	}
	if !hasActiveConsumers && !hasProducer {
		errs = append(errs, errors.New("no active consumers or producers configured"))
	}
	return errors.Join(errs...)
}
