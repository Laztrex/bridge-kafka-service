package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/mocksarama"
)

func probe(t *testing.T, srv *http.Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

// Liveness does not depend on Kafka; /health keeps working for existing probes.
func TestLivenessIndependentOfKafka(t *testing.T) {
	cfg := &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "out"}}}
	srv := server(t, cfg, router.NewProducerRegistry())
	for _, path := range []string{"/health", "/healthz"} {
		if w := probe(t, srv, http.MethodGet, path); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "running") {
			t.Errorf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	if w := probe(t, srv, http.MethodHead, "/healthz"); w.Code != http.StatusOK {
		t.Errorf("HEAD /healthz: %d", w.Code)
	}
}

func TestReadinessFollowsProducerAndConsumers(t *testing.T) {
	cfg := &schemas.KafkaInfo{
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
		Consumers: []*schemas.ConsumerConfig{{Topics: []string{"in"}}},
	}
	reg := router.NewProducerRegistry()
	ready := newReadiness(reg, cfg)
	srv := server(t, cfg, reg)
	srv.Handler = newHTTPServer(nil, reg, cfg, ready, "127.0.0.1:0").Handler

	w := probe(t, srv, http.MethodGet, "/readyz")
	if w.Code != http.StatusServiceUnavailable ||
		!strings.Contains(w.Body.String(), "producer is not connected") ||
		!strings.Contains(w.Body.String(), "consumers are running") {
		t.Fatalf("at startup: %d %s", w.Code, w.Body.String())
	}

	reg.Set(mocksarama.NewSyncProducer(t), []string{"out"})
	ready.consumerUp()
	if w := probe(t, srv, http.MethodGet, "/readyz"); w.Code != http.StatusOK {
		t.Fatalf("producer and consumer up: %d %s", w.Code, w.Body.String())
	}

	var st statusResponse
	w = probe(t, srv, http.MethodGet, "/status")
	if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if !st.Ready || !st.ProducerReady || st.ConsumersUp != 1 || st.ConsumersTotal != 1 {
		t.Fatalf("status: %+v", st)
	}
}

// Without producers in the config, readiness does not wait for one.
func TestReadinessWithoutProducers(t *testing.T) {
	r := newReadiness(router.NewProducerRegistry(), &schemas.KafkaInfo{})
	if reasons := r.reasons(); len(reasons) != 0 {
		t.Fatalf("want ready, got %v", reasons)
	}
}

func TestIsLoopback(t *testing.T) {
	for host, want := range map[string]bool{"127.0.0.1": true, "::1": true, "localhost": true, "0.0.0.0": false, "": false, "10.0.0.1": false} {
		if got := isLoopback(host); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", host, got, want)
		}
	}
}
