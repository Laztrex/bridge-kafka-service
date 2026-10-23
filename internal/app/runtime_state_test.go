package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

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
	ready := newRuntimeState(reg, cfg)
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
	r := newRuntimeState(router.NewProducerRegistry(), &schemas.KafkaInfo{})
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

// A stanza without topics is disabled: it does not count towards readiness.
func TestReadinessCountsOnlyActiveStanzas(t *testing.T) {
	cfg := &schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
		{Topics: []string{"a"}}, {Topics: []string{"b"}}, {Topics: nil}, nil,
	}}
	r := newRuntimeState(router.NewProducerRegistry(), cfg)
	if r.consumersTotal != 2 {
		t.Fatalf("consumers_total = %d, want 2", r.consumersTotal)
	}
	r.consumerUp()
	r.consumerUp()
	if reasons := r.reasons(); len(reasons) != 0 {
		t.Fatalf("both active stanzas are up, got reasons %v", reasons)
	}
}

func status(t *testing.T, r *runtimeState) statusResponse {
	t.Helper()
	w := httptest.NewRecorder()
	r.serveStatus(w, httptest.NewRequest(http.MethodGet, "/status", nil))
	var st statusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// /status lists every active pipeline with its retry state; a disabled stanza has no entry.
func TestStatus_PipelinesOfActiveStanzas(t *testing.T) {
	cfg := &schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
		{Topics: []string{"a"}, Group: "g1"}, {Topics: nil, Group: "off"}, {Topics: []string{"b", "c"}, Group: "g2"},
	}}
	r := newRuntimeState(router.NewProducerRegistry(), cfg)

	st := status(t, r)

	want := []pipelineStatus{{Group: "g1", Topics: []string{"a"}}, {Group: "g2", Topics: []string{"b", "c"}}}
	if !reflect.DeepEqual(st.Pipelines, want) {
		t.Fatalf("pipelines = %+v, want %+v", st.Pipelines, want)
	}
}

// A record stuck in retry shows in /status, and readiness does not change because of it.
func TestStatus_StuckRetryDoesNotAffectReadiness(t *testing.T) {
	cfg := &schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{{Topics: []string{"a"}, Group: "g"}}}
	r := newRuntimeState(router.NewProducerRegistry(), cfg)
	r.consumerUp()
	state := r.pipelines[0].state
	state.CallRetrying(&schemas.ConsumerEvent{}, time.Now().Add(-3*time.Minute))
	state.PublicationRetrying(&schemas.ConsumerEvent{}, time.Now().Add(-time.Minute))
	state.AddUncommitted(41)

	st := status(t, r)

	got := st.Pipelines[0]
	if got.RetryingCalls != 1 || got.RetryingPublications != 1 || got.Uncommitted != 41 {
		t.Fatalf("pipeline = %+v", got)
	}
	if got.OldestRetrySeconds < 179 || got.OldestRetrySeconds > 181 {
		t.Fatalf("oldest_retry_seconds = %d, want about 180", got.OldestRetrySeconds)
	}
	if !st.Ready || len(r.reasons()) != 0 {
		t.Fatalf("a retry must not affect readiness: %+v", st)
	}
}

// /status reports the process-wide pause in every pipeline entry; readiness ignores it.
func TestStatus_ReportsPause(t *testing.T) {
	cfg := &schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{{Topics: []string{"a"}, Group: "g1"}, {Topics: []string{"b"}, Group: "g2"}}}
	r := newRuntimeState(router.NewProducerRegistry(), cfg)
	r.consumerUp()
	r.consumerUp()

	for i, want := range []bool{false, true, false} {
		if i > 0 {
			r.pause.Toggle()
		}
		st := status(t, r)
		for _, p := range st.Pipelines {
			if p.Paused != want {
				t.Fatalf("after %d toggles: pipeline %s paused=%v, want %v", i, p.Group, p.Paused, want)
			}
		}
		if !st.Ready || len(r.reasons()) != 0 {
			t.Fatalf("a pause must not affect readiness: %+v", st)
		}
	}
}
