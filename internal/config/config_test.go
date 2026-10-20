package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

func TestValidate(t *testing.T) {
	consumer := &schemas.ConsumerConfig{Topics: []string{"t"}, Group: "g", Service: schemas.HostConfig{Name: "svc"}}
	cases := []struct {
		name    string
		cfg     *schemas.KafkaInfo
		wantErr bool
	}{
		{"no brokers", &schemas.KafkaInfo{}, true},
		{"nothing to do", &schemas.KafkaInfo{Brokers: []string{"b"}}, true},
		{"consumer ok", &schemas.KafkaInfo{Brokers: []string{"b"}, Consumers: []*schemas.ConsumerConfig{consumer}}, false},
		{"consumer without group", &schemas.KafkaInfo{Brokers: []string{"b"}, Consumers: []*schemas.ConsumerConfig{{Topics: []string{"t"}, Service: schemas.HostConfig{Name: "s"}}}}, true},
		{"consumer without service", &schemas.KafkaInfo{Brokers: []string{"b"}, Consumers: []*schemas.ConsumerConfig{{Topics: []string{"t"}, Group: "g"}}}, true},
		{"producer only", &schemas.KafkaInfo{Brokers: []string{"b"}, Producers: []*schemas.ProducerConfig{{Topic: "o"}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := Validate(c.cfg); (err != nil) != c.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}

// An empty consumer entry is an error, not a crash and not a disabled stanza; every problem is
// reported in one error.
func TestValidateReportsEveryProblem(t *testing.T) {
	disabled := &schemas.ConsumerConfig{Group: "g", Service: schemas.HostConfig{Name: "svc"}}
	active := &schemas.ConsumerConfig{Topics: []string{"t"}, Service: schemas.HostConfig{Name: "svc"}}

	err := Validate(&schemas.KafkaInfo{Brokers: []string{"b"}, Version: "2.x", Consumers: []*schemas.ConsumerConfig{active, nil, disabled}})

	for _, want := range []string{"consumer #2 is empty", "no Kafka consumer group defined for consumer #1", `kafka.version "2.x"`} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v must contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "consumer #3") {
		t.Fatalf("a stanza without topics is disabled, not an error: %v", err)
	}
}

// An empty producer entry is an error too, not a crash.
func TestValidateEmptyProducer(t *testing.T) {
	err := Validate(&schemas.KafkaInfo{Brokers: []string{"b"}, Producers: []*schemas.ProducerConfig{{Topic: "out"}, nil}})

	if err == nil || !strings.Contains(err.Error(), "producer #2 is empty") {
		t.Fatalf("error %v must name producer #2", err)
	}
}

// Consumer groups need Kafka 0.10.2 and record headers 0.11; a non-empty version must parse, an
// empty one keeps the client default.
func TestValidateKafkaVersion(t *testing.T) {
	consumer := &schemas.ConsumerConfig{Topics: []string{"t"}, Group: "g", Service: schemas.HostConfig{Name: "svc"}}
	producer := &schemas.ProducerConfig{Topic: "o"}
	consumers := []*schemas.ConsumerConfig{consumer}
	producers := []*schemas.ProducerConfig{producer}
	cases := []struct {
		name    string
		cfg     *schemas.KafkaInfo
		wantErr []string
	}{
		{"too old for groups", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "0.10.1.0", Consumers: consumers}, []string{`"0.10.1.0"`, "0.10.2"}},
		{"too old for groups, with producer", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "0.10.1.0", Consumers: consumers, Producers: producers}, []string{`"0.10.1.0"`, "0.10.2"}},
		{"group minimum, consumers only", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "0.10.2.0", Consumers: consumers}, nil},
		{"too old for headers", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "0.10.2.0", Consumers: consumers, Producers: producers}, []string{`"0.10.2.0"`, "0.11"}},
		{"too old for headers, producer only", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "0.10.1.0", Producers: producers}, []string{`"0.10.1.0"`, "0.11"}},
		{"header minimum", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "0.11.0.0", Consumers: consumers, Producers: producers}, nil},
		{"unparseable stops startup", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "latest", Consumers: consumers, Producers: producers}, []string{`"latest"`, "not a Kafka version"}},
		{"unparseable without consumers or producers checks", &schemas.KafkaInfo{Brokers: []string{"b"}, Version: "2.x", Producers: producers}, []string{`"2.x"`}},
		{"empty keeps the default", &schemas.KafkaInfo{Brokers: []string{"b"}, Consumers: consumers, Producers: producers}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate(c.cfg)
			if len(c.wantErr) == 0 {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			for _, want := range c.wantErr {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v must contain %q", err, want)
				}
			}
		})
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	cfg := mustLoad(t, "kafka:\n  brokers: b:9092\nconsumer:\n  - topics: t\n    group: g\n    service:\n      name: svc\nproducer:\n  - topic: out\n", nil)

	if cfg.LogLevel != DefaultLogLevel || cfg.HTTPAddr != DefaultHTTPAddr || cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Fatalf("defaults: %q %q %v", cfg.LogLevel, cfg.HTTPAddr, cfg.ShutdownTimeout)
	}
	if !reflect.DeepEqual(cfg.Kafka.Brokers, []string{"b:9092"}) || cfg.Kafka.Producers[0].Topic != "out" {
		t.Fatalf("kafka: %+v", cfg.Kafka)
	}
	svc := cfg.Kafka.Consumers[0].Service
	if svc.Timeout != consts.DefaultServiceTimeout || svc.Port != consts.PortServiceDefault || svc.Path != consts.BasePathServiceDefault {
		t.Fatalf("service defaults: %+v", svc)
	}
	if cfg.MetricFilePath() != filepath.Join(consts.MetricPath, consts.MetricFile) {
		t.Fatalf("metric file: %s", cfg.MetricFilePath())
	}
	if cfg.Kafka.TLSConf == nil {
		t.Fatal("the Kafka TLS settings are always built")
	}
}

func TestLoadReadsUpstreamProtocolAndTLSKeys(t *testing.T) {
	cfg := mustLoad(t, `consumer:
  - topics: t
    group: g
    service:
      name: svc
      port: "50051"
      protocol: grpc
      tls:
        mode: mtls
        ca_path: ca.pem
        cert_path: c.pem
        key_path: c.key
        server_name: svc.internal
        skip_verify: true
`, nil)
	svc := cfg.Kafka.Consumers[0].Service
	want := schemas.ServiceTLS{Mode: "mtls", CAPath: "ca.pem", CertPath: "c.pem", KeyPath: "c.key", ServerName: "svc.internal", SkipVerify: true}
	if svc.Protocol != "grpc" || svc.TLS != want {
		t.Fatalf("service: %+v", svc)
	}
}

func TestLoadOmittedUpstreamKeysMeanHTTPPlaintext(t *testing.T) {
	svc := mustLoad(t, "consumer:\n  - topics: t\n    group: g\n    service:\n      name: svc\n", nil).Kafka.Consumers[0].Service
	if svc.EffectiveProtocol() != "http" || svc.TLS.Encrypted() {
		t.Fatalf("service: %+v", svc)
	}
}

func TestServicePortFromEnvironmentOrFile(t *testing.T) {
	yaml := "consumer:\n  - topics: t\n    group: g\n    service:\n      name: my-svc\n"
	if got := mustLoad(t, yaml, map[string]string{"MY_SVC_SERVICE_PORT": "9001"}).Kafka.Consumers[0].Service.Port; got != "9001" {
		t.Errorf("from env: %q", got)
	}
	if got := mustLoad(t, yaml+"MY_SVC_SERVICE_PORT: 9002\n", nil).Kafka.Consumers[0].Service.Port; got != "9002" {
		t.Errorf("from file: %q", got)
	}
	if got := mustLoad(t, yaml, nil).Kafka.Consumers[0].Service.Port; got != consts.PortServiceDefault {
		t.Errorf("default: %q", got)
	}
}

func TestShutdownTimeout(t *testing.T) {
	if got := mustLoad(t, "timeouts:\n  shutdown: 25s\n", nil).ShutdownTimeout; got != 25*time.Second {
		t.Errorf("from file: %v", got)
	}
	if got := mustLoad(t, "", map[string]string{"TIMEOUTS_SHUTDOWN": "3s"}).ShutdownTimeout; got != 3*time.Second {
		t.Errorf("from env: %v", got)
	}
	cfg := mustLoad(t, "timeouts:\n  shutdown: 25s\nconsumer:\n  - topics: t\n    group: g\n    service:\n      name: s\n", nil)
	if got := cfg.Kafka.Consumers[0].DrainWindow; got != 25*time.Second {
		t.Errorf("active upstream calls get timeouts.shutdown to finish, got %v", got)
	}
	for _, bad := range []string{"0s", "-1s", "5", "soon"} {
		if _, err := Load(writeFile(t, t.TempDir(), "app.yaml", "timeouts:\n  shutdown: "+bad+"\n"), env(nil)); err == nil {
			t.Errorf("timeouts.shutdown: %s must be rejected", bad)
		}
	}
}

// Keys the loader does not know are reported, not rejected: files written for older versions keep loading.
func TestUnknownKeysAreWarnings(t *testing.T) {
	cfg := mustLoad(t, `logLevel: info
legacy_flag: true
kafka:
  brokers: b
  tls:
    ca:
      path: ca.pem
consumer:
  - topics: t
    group: g
    retries: 3
    service:
      name: s
APP_SVC_SERVICE_PORT: 1
LOG_PATH: /x
`, nil)
	want := []string{"consumer[0].retries", "legacy_flag"}
	if len(cfg.Warnings) != len(want) {
		t.Fatalf("warnings: %v", cfg.Warnings)
	}
	for i, w := range want {
		if !strings.HasSuffix(cfg.Warnings[i], w) {
			t.Fatalf("warnings: %v, want %v", cfg.Warnings, want)
		}
	}
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml"), env(nil)); err == nil {
		t.Error("a missing file must fail")
	}
	if _, err := Load(writeFile(t, t.TempDir(), "bad.yaml", "kafka: [\n"), env(nil)); err == nil {
		t.Error("broken YAML must fail")
	}
	if _, err := Load(writeFile(t, t.TempDir(), "bad.yaml", "consumer:\n  - service: oops\n"), env(nil)); err == nil {
		t.Error("an undecodable section must fail")
	}
}

func TestResolvePath(t *testing.T) {
	t.Chdir(t.TempDir())
	if p, def := ResolvePath("", env(nil)); p != DefaultPath || !def {
		t.Errorf("default: %q %v", p, def)
	}
	writeFile(t, ".", ".env", "# comment\nexport CONFIG_PATH=\"/from/dotenv.yaml\"\n")
	if p, _ := ResolvePath("", env(nil)); p != "/from/dotenv.yaml" {
		t.Errorf(".env: %q", p)
	}
	if p, _ := ResolvePath("", env(map[string]string{"CONFIG_PATH": "/from/env.yaml"})); p != "/from/env.yaml" {
		t.Errorf("env beats .env: %q", p)
	}
	if p, def := ResolvePath("/from/flag.yaml", env(map[string]string{"CONFIG_PATH": "/from/env.yaml"})); p != "/from/flag.yaml" || def {
		t.Errorf("flag beats env: %q", p)
	}
}

// Like the former loader, which opened the run log before reading the config, the run log
// location honours the environment only.
func TestLogFilePath(t *testing.T) {
	if got := LogFilePath(env(nil)); got != filepath.Join(consts.LogPath, consts.LogFile) {
		t.Errorf("default: %s", got)
	}
	if got := LogFilePath(env(map[string]string{"LOG_PATH": "/l", "LOG_FILE_NAME": "x.log"})); got != "/l/x.log" {
		t.Errorf("env: %s", got)
	}
}

func TestLogKafkaConfigDoesNotPanic(t *testing.T) {
	LogKafkaConfig(nil)
	LogKafkaConfig(&schemas.KafkaInfo{
		Brokers:   []string{"b"},
		Consumers: []*schemas.ConsumerConfig{nil, {Topics: []string{"t"}, Service: schemas.HostConfig{Name: "s", Port: "1"}}},
		Producers: []*schemas.ProducerConfig{{Topic: "o"}, nil},
	})
}

// The configs under examples/ are copied by users: they must stay loadable and valid, and
// must reference the files scripts/gen-demo-certs.sh produces.
func TestExampleConfigs(t *testing.T) {
	load := func(name string) *Config {
		cfg, err := Load(filepath.Join("..", "..", "examples", name), env(nil))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(cfg.Warnings) != 0 {
			t.Errorf("%s: unknown keys %v", name, cfg.Warnings)
		}
		return cfg
	}
	for _, name := range []string{"config_local_http.yaml", "config_grpc.yaml"} {
		k := load(name).Kafka
		if err := Validate(k); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if err := PrepareUpstreams(k); err != nil {
			t.Errorf("%s: plaintext examples need no files: %v", name, err)
		}
		for _, c := range k.Consumers {
			listed := false
			for _, p := range k.Producers {
				listed = listed || p.Topic == c.ResponseTopic
			}
			if !listed {
				t.Errorf("%s: response_topic %q must be a producer topic", name, c.ResponseTopic)
			}
		}
	}
	if svc := load("config_grpc.yaml").Kafka.Consumers[0].Service; svc.EffectiveProtocol() != "grpc" || svc.TLS.Encrypted() {
		t.Errorf("config_grpc.yaml: %+v", svc)
	}

	k := load("config_grpc_mtls.yaml").Kafka
	if err := Validate(k); err != nil {
		t.Fatal(err)
	}
	tlsConf := k.Consumers[0].Service.TLS
	if tlsConf.EffectiveMode() != "mtls" || tlsConf.CAPath != "certs/demo/ca.pem" ||
		tlsConf.CertPath != "certs/demo/client.pem" || tlsConf.KeyPath != "certs/demo/client.key" {
		t.Fatalf("mtls example: %+v", tlsConf)
	}
	if _, err := os.Stat(filepath.Join("..", "..", "certs", "demo", "client.pem")); os.IsNotExist(err) {
		if err := PrepareUpstreams(k); err == nil {
			t.Error("the mtls example must fail without its certificates")
		}
	}
}

// service.timeout: a number is seconds, a string is a Go duration, unset is 30 seconds. (The former
// loader multiplied a second time and turned "10s" into an immediate timeout and unset into none.)
func TestServiceTimeout(t *testing.T) {
	for yaml, want := range map[string]time.Duration{
		"":               consts.DefaultServiceTimeout,
		"timeout: 10\n":  10 * time.Second,
		"timeout: 1.5\n": 1500 * time.Millisecond,
		"timeout: 10s\n": 10 * time.Second,
		"timeout: 2m\n":  2 * time.Minute,
	} {
		cfg := mustLoad(t, "consumer:\n  - topics: t\n    group: g\n    service:\n      name: s\n      "+yaml, nil)
		if got := cfg.Kafka.Consumers[0].Service.Timeout; got != want {
			t.Errorf("%q: got %v, want %v", yaml, got, want)
		}
	}
}

type compatConsumer struct {
	Topics        []string
	Group         string
	ClientID      string
	ResponseTopic string
	Name          string
	Port          string
	Path          string
	MaxConn       int
	TimeoutNs     int64
	Protocol      string
	TLSMode       string
	TLSCAPath     string
	TLSCertPath   string
	TLSKeyPath    string
	TLSServerName string
	TLSSkipVerify bool
}

type compatSnapshot struct {
	LogLevel      string
	HTTPAddr      string
	PprofAddr     string
	AuditEndpoint string
	KafkaTLSCert  string
	KafkaTLSKey   string
	KafkaTLSCA    string
	KafkaTLSSkip  bool
	Brokers       []string
	Version       string
	Consumers     []compatConsumer
	Producers     []string
	LogPath       string
	LogFileName   string
	MetricPath    string
	MetricFile    string
	SystemID      string
}

type compatCase struct {
	Name string
	File string
	Env  map[string]string
}

// The loader must produce exactly what the former viper-based loader did for the snapshots in
// testdata/compat: existing deployments rely on it. The one intended change is service.timeout,
// which the former loader mis-scaled (see TestServiceTimeout).
func TestLoadMatchesFormerLoader(t *testing.T) {
	dir := filepath.Join("testdata", "compat")
	raw, err := os.ReadFile(filepath.Join(dir, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []compatCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var want compatSnapshot
			raw, err := os.ReadFile(filepath.Join(dir, c.Name+".golden.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			for i := range want.Consumers {
				switch ns := want.Consumers[i].TimeoutNs; {
				case ns == int64(300*time.Second):
					want.Consumers[i].TimeoutNs = int64(consts.DefaultServiceTimeout)
				case ns < int64(time.Millisecond):
					want.Consumers[i].TimeoutNs = ns * int64(time.Second)
				}
			}
			if want.MetricPath == "" {
				want.MetricPath = consts.MetricPath
			}
			if want.MetricFile == "" {
				want.MetricFile = consts.MetricFile
			}

			cfg, err := Load(filepath.Join(dir, c.File), env(c.Env))
			if err != nil {
				t.Fatal(err)
			}
			got := compatSnapshot{
				LogLevel: cfg.LogLevel, HTTPAddr: cfg.HTTPAddr, PprofAddr: cfg.PprofAddr, AuditEndpoint: cfg.AuditGRPCEndpoint,
				KafkaTLSCert: cfg.KafkaTLS.CertPath, KafkaTLSKey: cfg.KafkaTLS.KeyPath, KafkaTLSCA: cfg.KafkaTLS.CAPath,
				KafkaTLSSkip: cfg.KafkaTLS.SkipVerify, Brokers: cfg.Kafka.Brokers, Version: cfg.Kafka.Version,
				LogPath: want.LogPath, LogFileName: want.LogFileName,
				MetricPath: cfg.MetricPath, MetricFile: cfg.MetricFileName, SystemID: cfg.SystemID,
			}
			for _, k := range cfg.Kafka.Consumers {
				got.Consumers = append(got.Consumers, compatConsumer{
					Topics: k.Topics, Group: k.Group, ClientID: k.ClientId, ResponseTopic: k.ResponseTopic,
					Name: k.Service.Name, Port: k.Service.Port, Path: k.Service.Path, MaxConn: k.Service.MaxConn,
					TimeoutNs: int64(k.Service.Timeout), Protocol: k.Service.Protocol,
					TLSMode: k.Service.TLS.Mode, TLSCAPath: k.Service.TLS.CAPath, TLSCertPath: k.Service.TLS.CertPath,
					TLSKeyPath: k.Service.TLS.KeyPath, TLSServerName: k.Service.TLS.ServerName, TLSSkipVerify: k.Service.TLS.SkipVerify,
				})
			}
			for _, p := range cfg.Kafka.Producers {
				got.Producers = append(got.Producers, p.Topic)
			}
			if !reflect.DeepEqual(got, want) {
				g, _ := json.MarshalIndent(got, "", "  ")
				w, _ := json.MarshalIndent(want, "", "  ")
				t.Fatalf("differs from the former loader\n got: %s\nwant: %s", g, w)
			}
		})
	}
}

// env returns a getenv over m (absent keys are empty, as unset variables).
func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustLoad(t *testing.T, yaml string, vars map[string]string) *Config {
	t.Helper()
	cfg, err := Load(writeFile(t, t.TempDir(), "app.yaml", yaml), env(vars))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

// Backup retention: 0 by default (keep everything), whole numbers from YAML or strings, anything
// else stops loading with the key and the value.
func TestRetention(t *testing.T) {
	if got := mustLoad(t, "", nil).Retention; got != (Retention{}) {
		t.Fatalf("defaults: %+v", got)
	}
	cfg := mustLoad(t, "retention:\n  max_backups: 10\n  max_age_days: \"30\"\n", nil)
	if cfg.Retention != (Retention{MaxBackups: 10, MaxAgeDays: 30}) {
		t.Fatalf("parsed: %+v", cfg.Retention)
	}
	for _, w := range cfg.Warnings {
		if strings.HasPrefix(w, "retention") {
			t.Fatalf("retention keys must be known, got warning %q", w)
		}
	}
	for _, bad := range []string{"-1", "month"} {
		_, err := Load(writeFile(t, t.TempDir(), "app.yaml", "retention:\n  max_age_days: "+bad+"\n"), env(nil))
		if err == nil || !strings.Contains(err.Error(), "retention.max_age_days") || !strings.Contains(err.Error(), bad) {
			t.Fatalf("value %q: error %v must name the key and the value", bad, err)
		}
	}
}
