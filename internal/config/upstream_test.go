package config

import (
	"bytes"
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/testcerts"
)

func TestNewUpstreamTLSDefaultIsPlaintext(t *testing.T) {
	for _, c := range []schemas.ServiceTLS{{}, {Mode: "none", CAPath: "/ignored"}} {
		cfg, err := NewUpstreamTLS("s", c)
		if err != nil || cfg != nil {
			t.Fatalf("%+v: no encryption expected, got %v %v", c, cfg, err)
		}
	}
}

func TestNewUpstreamTLSWithCA(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	cfg, err := NewUpstreamTLS("s", schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath, ServerName: "svc.internal"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 0 || cfg.InsecureSkipVerify || cfg.ServerName != "svc.internal" || cfg.MinVersion < tls.VersionTLS12 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestNewUpstreamTLSSystemRootsWhenNoCA(t *testing.T) {
	cfg, err := NewUpstreamTLS("s", schemas.ServiceTLS{Mode: "tls"})
	if err != nil || cfg.RootCAs != nil {
		t.Fatalf("nil RootCAs (system roots) expected: %v %v", cfg, err)
	}
}

func TestNewUpstreamTLSMutual(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	cfg, err := NewUpstreamTLS("s", schemas.ServiceTLS{Mode: "mtls", CAPath: b.CAPath, CertPath: b.ClientCertPath, KeyPath: b.ClientKeyPath})
	if err != nil || len(cfg.Certificates) != 1 {
		t.Fatalf("client certificate expected: %v %v", cfg, err)
	}
}

func TestNewUpstreamTLSFailsClosed(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	garbage := writeFile(t, t.TempDir(), "garbage.pem", "not pem")
	missing := filepath.Join(t.TempDir(), "absent.pem")
	cases := []struct {
		name string
		cfg  schemas.ServiceTLS
		want string
	}{
		{"unknown mode", schemas.ServiceTLS{Mode: "TLS1"}, "unknown service.tls.mode"},
		{"missing CA file", schemas.ServiceTLS{Mode: "tls", CAPath: missing}, "cannot read service.tls.ca_path"},
		{"malformed CA", schemas.ServiceTLS{Mode: "tls", CAPath: garbage}, "no valid PEM certificate"},
		{"mtls without key", schemas.ServiceTLS{Mode: "mtls", CertPath: b.ClientCertPath}, "missing key_path"},
		{"mtls without cert", schemas.ServiceTLS{Mode: "mtls", KeyPath: b.ClientKeyPath}, "missing cert_path"},
		{"mtls with malformed pair", schemas.ServiceTLS{Mode: "mtls", CertPath: garbage, KeyPath: garbage}, "cannot load client certificate"},
		{"mtls with missing files", schemas.ServiceTLS{Mode: "mtls", CertPath: missing, KeyPath: missing}, "cannot load client certificate"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := NewUpstreamTLS("consumer #1", c.cfg)
			if cfg != nil {
				t.Fatal("never a weakened configuration")
			}
			if err == nil || !strings.Contains(err.Error(), "consumer #1") || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v must name the stanza and contain %q", err, c.want)
			}
		})
	}
}

func TestNewUpstreamTLSSkipVerifyIsLoudlyWarned(t *testing.T) {
	var buf bytes.Buffer
	old := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = old })

	cfg, err := NewUpstreamTLS("consumer #7", schemas.ServiceTLS{Mode: "tls", SkipVerify: true})
	if err != nil || !cfg.InsecureSkipVerify {
		t.Fatalf("%v %v", cfg, err)
	}
	if !strings.Contains(buf.String(), "consumer #7") || !strings.Contains(buf.String(), "NOT verified") {
		t.Fatalf("warning missing: %s", buf.String())
	}
}

// httpService is a valid HTTP address, as config.Load fills it from defaults.
var httpService = schemas.HostConfig{Name: "svc", Port: "8080", Path: "/predict"}

func TestPrepareUpstreams(t *testing.T) {
	b := testcerts.New(t, t.TempDir())

	c := &schemas.ConsumerConfig{Topics: []string{"t"}, Service: httpService}
	if err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{c}}); err != nil || c.UpstreamTLS != nil {
		t.Fatalf("defaults keep plaintext: %v %v", c.UpstreamTLS, err)
	}

	c = &schemas.ConsumerConfig{Service: schemas.HostConfig{Protocol: "grpc", TLS: schemas.ServiceTLS{Mode: "tls", CAPath: b.CAPath}}}
	if err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{c}}); err != nil || c.UpstreamTLS == nil {
		t.Fatalf("tls config per stanza: %v %v", c.UpstreamTLS, err)
	}

	c = &schemas.ConsumerConfig{Topics: []string{"orders"}, Service: schemas.HostConfig{Protocol: "soap"}}
	err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{nil, c}})
	for _, want := range []string{"consumer #2", "orders", `"soap"`, "allowed: http, grpc"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("unknown protocol error %v must contain %q", err, want)
		}
	}

	c = &schemas.ConsumerConfig{Service: schemas.HostConfig{TLS: schemas.ServiceTLS{Mode: "tls", CAPath: "/nonexistent.pem"}}}
	if PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{c}}) == nil {
		t.Fatal("bad tls material must abort")
	}
}

func TestPrepareUpstreamsFailurePolicy(t *testing.T) {
	cases := []struct {
		name    string
		policy  string
		maxConn int
		want    []string
	}{
		{"default", "", -1, nil},
		{"skip without max_conn", "skip", 0, nil},
		{"retry with max_conn", "Retry", 3, nil},
		{"retry without max_conn", "retry", -1, []string{"consumer #1", "orders", "service.max_conn"}},
		{"retry with zero max_conn", "retry", 0, []string{"service.max_conn"}},
		{"unknown", "park", 3, []string{"consumer #1", `"park"`, "allowed: skip, retry"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc := httpService
			svc.MaxConn = c.maxConn
			cc := &schemas.ConsumerConfig{Topics: []string{"orders"}, OnFailure: c.policy, Service: svc}
			err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{cc}})
			if len(c.want) == 0 {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			for _, want := range c.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v must contain %q", err, want)
				}
			}
		})
	}
}

// An active gRPC stanza needs a host:port target the gRPC client can parse; an HTTP stanza is held to
// the same host and port rule, and stanzas without topics are not checked.
func TestPrepareUpstreamsGRPCTarget(t *testing.T) {
	cases := []struct {
		name     string
		protocol string
		host     string
		port     string
		topics   []string
		want     []string
	}{
		{"valid", "grpc", "model-svc", "50051", []string{"orders"}, nil},
		{"empty port", "grpc", "model-svc", "", []string{"orders"}, []string{"consumer #1", "orders", "service.port"}},
		{"non-numeric port", "grpc", "model-svc", "abc", []string{"orders"}, []string{`"abc"`}},
		{"zero port", "grpc", "model-svc", "0", []string{"orders"}, []string{`"0"`}},
		{"port out of range", "grpc", "model-svc", "70000", []string{"orders"}, []string{`"70000"`}},
		{"signed port", "grpc", "model-svc", "+80", []string{"orders"}, []string{`"+80"`}},
		{"host with space", "grpc", "model svc", "50051", []string{"orders"}, []string{"service.name"}},
		{"http checked by the same rule", "http", "svc", "abc", []string{"orders"}, []string{"consumer #1", `"abc"`, "for http"}},
		{"inactive not checked", "grpc", "", "", nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cc := &schemas.ConsumerConfig{Topics: c.topics, Service: schemas.HostConfig{Protocol: c.protocol, Name: c.host, Port: c.port, Path: "/"}}
			err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{cc}})
			if len(c.want) == 0 {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			for _, want := range c.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v must contain %q", err, want)
				}
			}
		})
	}
}

// A retry stanza names a response_topic that no producer serves: such a config
// would lose every response, so startup must fail and name both.
func TestPrepareUpstreamsRetryResponseTopicMustBeProduced(t *testing.T) {
	cases := []struct {
		name          string
		policy        string
		responseTopic string
		producers     []*schemas.ProducerConfig
		want          []string
	}{
		{"retry with unlisted response topic", "retry", "orders.out", nil,
			[]string{"consumer #1", "orders", `"orders.out"`, "producer topic"}},
		{"retry with listed response topic", "retry", "orders.out",
			[]*schemas.ProducerConfig{{Topic: "orders.out"}}, nil},
		{"retry without response topic", "retry", "", nil, nil},
		{"skip with unlisted response topic", "skip", "orders.out", nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cc := &schemas.ConsumerConfig{
				Topics:        []string{"orders"},
				OnFailure:     c.policy,
				ResponseTopic: c.responseTopic,
				Service:       schemas.HostConfig{Name: "svc", Port: "8080", Path: "/predict", MaxConn: 2},
			}
			err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{cc}, Producers: c.producers})
			if len(c.want) == 0 {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			for _, want := range c.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v must contain %q", err, want)
				}
			}
		})
	}
}

// Kafka broker TLS keeps its historical behaviour: broken material degrades to an insecure
// connection with a warning instead of stopping startup.
func TestNewKafkaTLS(t *testing.T) {
	b := testcerts.New(t, t.TempDir())
	read := func(p string) string {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	cert, key, ca := read(b.ClientCertPath), read(b.ClientKeyPath), read(b.CAPath)
	material := func(cert, key, ca string) KafkaTLS {
		dir := t.TempDir()
		k := KafkaTLS{CertPath: filepath.Join(dir, "absent-cert"), KeyPath: filepath.Join(dir, "absent-key"), CAPath: filepath.Join(dir, "absent-ca")}
		if cert != "" {
			k.CertPath = writeFile(t, dir, "cert.pem", cert)
		}
		if key != "" {
			k.KeyPath = writeFile(t, dir, "key.pem", key)
		}
		if ca != "" {
			k.CAPath = writeFile(t, dir, "ca.pem", ca)
		}
		return k
	}

	full := material(cert, key, ca)
	cfg := NewKafkaTLS(full)
	if cfg.InsecureSkipVerify || len(cfg.Certificates) != 1 || cfg.RootCAs == nil {
		t.Fatalf("full material must give mutual TLS: %+v", cfg)
	}
	full.SkipVerify = true
	if !NewKafkaTLS(full).InsecureSkipVerify {
		t.Fatal("an explicit bypass is honoured")
	}

	for name, k := range map[string]KafkaTLS{
		"missing cert":   material("", key, ca),
		"missing key":    material(cert, "", ca),
		"malformed pair": material("garbage", "garbage", ca),
		"missing CA":     material(cert, key, ""),
		"malformed CA":   material(cert, key, "not pem"),
	} {
		if cfg := NewKafkaTLS(k); !cfg.InsecureSkipVerify || len(cfg.Certificates) != 0 {
			t.Errorf("%s: must degrade to insecure, got %+v", name, cfg)
		}
	}
}

// An active HTTP stanza needs an address every call can use; nothing is resolved.
func TestPrepareUpstreamsHTTPTarget(t *testing.T) {
	cases := []struct {
		name   string
		svc    schemas.HostConfig
		topics []string
		want   []string
	}{
		{"valid", httpService, []string{"orders"}, nil},
		{"unresolvable host is fine", schemas.HostConfig{Name: "model-svc.invalid", Port: "8080", Path: "/"}, []string{"orders"}, nil},
		{"non-numeric port", schemas.HostConfig{Name: "svc", Port: "abc", Path: "/"}, []string{"orders"}, []string{"consumer #1", "orders", `"abc"`}},
		{"zero port", schemas.HostConfig{Name: "svc", Port: "0", Path: "/"}, []string{"orders"}, []string{`"0"`}},
		{"port out of range", schemas.HostConfig{Name: "svc", Port: "70000", Path: "/"}, []string{"orders"}, []string{`"70000"`}},
		{"path without slash", schemas.HostConfig{Name: "svc", Port: "8080", Path: "predict"}, []string{"orders"}, []string{"consumer #1", `"predict"`, "service.path"}},
		{"host with space", schemas.HostConfig{Name: "my svc", Port: "8080", Path: "/"}, []string{"orders"}, []string{"service.name"}},
		{"bad URL", schemas.HostConfig{Name: "svc%zz", Port: "8080", Path: "/"}, []string{"orders"}, []string{"does not form an HTTP request"}},
		{"inactive not checked", schemas.HostConfig{Port: "abc"}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{{Topics: c.topics, Service: c.svc}}})
			if len(c.want) == 0 {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			for _, want := range c.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v must contain %q", err, want)
				}
			}
		})
	}
}

// Every problem of every stanza is reported together.
func TestPrepareUpstreamsReportsEveryProblem(t *testing.T) {
	bad := &schemas.ConsumerConfig{Topics: []string{"orders"}, Service: schemas.HostConfig{
		Name: "svc", Port: "abc", Path: "/", TLS: schemas.ServiceTLS{Mode: "mtls", CertPath: "/c.pem"},
	}}
	other := &schemas.ConsumerConfig{Topics: []string{"payments"}, OnFailure: "park", Service: httpService}

	err := PrepareUpstreams(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{bad, other}})

	for _, want := range []string{`service.port "abc"`, "missing key_path", "consumer #2", `"park"`} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v must contain %q", err, want)
		}
	}
}
