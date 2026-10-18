package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/fakeclock"
	"bridge-kafka-service/testutil/metricsfile"
	"bridge-kafka-service/testutil/mocksarama"
)

// ctxSession is a ConsumerGroupSession with a cancellable context that records marks.
type ctxSession struct {
	sarama.ConsumerGroupSession
	ctx    context.Context
	mu     sync.Mutex
	marked []int64
}

func (s *ctxSession) Context() context.Context { return s.ctx }
func (s *ctxSession) MarkMessage(m *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, m.Offset)
}
func (s *ctxSession) Marked() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.marked...)
}

type claim struct {
	msgs chan *sarama.ConsumerMessage
}

func (c *claim) Topic() string                            { return "in" }
func (c *claim) Partition() int32                         { return 0 }
func (c *claim) InitialOffset() int64                     { return 0 }
func (c *claim) HighWaterMarkOffset() int64               { return 0 }
func (c *claim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }

func newTestLogger(t *testing.T) *observability.FileTransactionLogger {
	t.Helper()
	l, err := observability.NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	l.Run()
	l.RunAudit()
	t.Cleanup(l.Shutdown)
	return l
}

type claimFixture struct {
	consumer *Consumer
	session  *ctxSession
	claim    *claim
	ch       chan *schemas.ConsumerEvent
	cancel   context.CancelFunc
}

func newClaimFixture(t *testing.T, chSize int) *claimFixture {
	t.Helper()
	ch := make(chan *schemas.ConsumerEvent, chSize)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &claimFixture{
		consumer: &Consumer{
			config: &schemas.ConsumerConfig{Service: schemas.HostConfig{Path: "/predict"}, ResponseTopic: "resp"},
			logger: newTestLogger(t),
			out:    ch,
			ready:  make(chan bool),
		},
		session: &ctxSession{ctx: ctx},
		claim:   &claim{msgs: make(chan *sarama.ConsumerMessage, 8)},
		ch:      ch,
		cancel:  cancel,
	}
}

func (f *claimFixture) run() <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.consumer.ConsumeClaim(f.session, f.claim) }()
	return done
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("ConsumeClaim did not return")
		return nil
	}
}

func TestConsumeClaim_DeliversMessageAndSettlesOnlyAfterAck(t *testing.T) {
	f := newClaimFixture(t, 4)
	done := f.run()

	f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: 5, Value: []byte(`{"a":1}`)}

	var ev *schemas.ConsumerEvent
	select {
	case ev = <-f.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("event not delivered")
	}
	assert.JSONEq(t, `{"a":1}`, string(ev.Values.Body.Data))
	assert.Equal(t, "resp", ev.ResponseTopic)
	assert.Equal(t, "/predict", ev.Values.Path)
	assert.Equal(t, "in", ev.Values.Topic)
	assert.Empty(t, f.session.Marked(), "offset must not be marked before the message is processed")

	ev.Ack()
	assert.Equal(t, []int64{5}, f.session.Marked())

	f.cancel()
	assert.NoError(t, waitDone(t, done))
}

func TestConsumeClaim_CancelledWhileHandingOverReturns(t *testing.T) {
	f := newClaimFixture(t, 0)
	done := f.run()
	f.claim.msgs <- &sarama.ConsumerMessage{Offset: 1, Value: []byte(`{}`)}

	time.Sleep(50 * time.Millisecond)
	f.cancel()

	assert.NoError(t, waitDone(t, done))
	assert.Empty(t, f.session.Marked())
}

func TestConsumeClaim_ClosedMessageChannelReturns(t *testing.T) {
	f := newClaimFixture(t, 1)
	done := f.run()
	close(f.claim.msgs)
	assert.NoError(t, waitDone(t, done))
}

func TestSetupSignalsReadinessOnceAndCleanupSucceeds(t *testing.T) {
	c := &Consumer{ready: make(chan bool)}

	assert.NoError(t, c.Setup(nil))
	assert.NoError(t, c.Setup(nil), "a rebalance must not close the channel twice")
	select {
	case <-c.ready:
	default:
		t.Fatal("ready channel was not closed")
	}
	assert.NoError(t, c.Cleanup(nil))
}

func upstream(t *testing.T) (*httptest.Server, schemas.HostConfig) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	return srv, schemas.HostConfig{Name: u.Hostname(), Port: u.Port(), Path: "/predict", Timeout: 2 * time.Second}
}

func useFakeGroup(t *testing.T) *mocksarama.FakeConsumerGroup {
	t.Helper()
	g := mocksarama.NewFakeConsumerGroup(nil)
	old := newConsumerGroup
	newConsumerGroup = func(string, sarama.Client) (sarama.ConsumerGroup, error) { return g, nil }
	t.Cleanup(func() { newConsumerGroup = old })
	return g
}

func TestRunSubscription_ProcessesMessagesTogglesPauseAndStopsOnCancel(t *testing.T) {
	g := useFakeGroup(t)
	_, host := upstream(t)
	conf := &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	toggle := make(chan struct{}, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunSubscription(ctx, toggle, mocksarama.NewFakeClient(), conf, newTestLogger(t))
	}()

	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 3, Value: []byte(`{"q":1}`)})
	require.Eventually(t, func() bool { return len(g.MarkedOffsets()) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []int64{3}, g.MarkedOffsets())

	toggle <- struct{}{}
	require.Eventually(t, func() bool { return g.PauseCount() == 1 }, 3*time.Second, 10*time.Millisecond)
	toggle <- struct{}{}
	require.Eventually(t, func() bool { return g.ResumeCount() == 1 }, 3*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunSubscription did not stop on cancel")
	}
}

func TestRunSubscription_SlowModeAndGroupCreationFailure(t *testing.T) {
	_, host := upstream(t)
	host.MaxConn = 2
	conf := &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host}

	old := newConsumerGroup
	newConsumerGroup = func(string, sarama.Client) (sarama.ConsumerGroup, error) { return nil, errors.New("no group") }
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunSubscription(context.Background(), nil, mocksarama.NewFakeClient(), conf, newTestLogger(t))
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunSubscription must return when the group cannot be created")
	}
	newConsumerGroup = old

	g := useFakeGroup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		RunSubscription(ctx, nil, mocksarama.NewFakeClient(), conf, newTestLogger(t))
	}()
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 1, Value: []byte(`{}`)})
	require.Eventually(t, func() bool { return len(g.MarkedOffsets()) == 1 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done2
}

// A subscription that cannot start returns an error naming its topics, so the caller can treat
// it as fatal; a subscription cancelled before it starts is a clean stop.
func TestRunSubscription_StartFailuresReturnErrorsAndCleanStopReturnsNil(t *testing.T) {
	_, host := upstream(t)

	old := newConsumerGroup
	newConsumerGroup = func(string, sarama.Client) (sarama.ConsumerGroup, error) { return nil, errors.New("no group") }
	err := RunSubscription(context.Background(), nil, mocksarama.NewFakeClient(),
		&schemas.ConsumerConfig{Topics: []string{"orders"}, Group: "g", Service: host}, newTestLogger(t))
	newConsumerGroup = old
	require.Error(t, err)
	assert.ErrorContains(t, err, "orders")
	assert.ErrorContains(t, err, "cannot create consumer group")
	assert.ErrorContains(t, err, "no group")

	badTarget := schemas.HostConfig{Name: "%zz", Port: "1", Protocol: schemas.ProtocolGRPC}
	err = RunSubscription(context.Background(), nil, mocksarama.NewFakeClient(),
		&schemas.ConsumerConfig{Topics: []string{"payments"}, Group: "g", Service: badTarget}, newTestLogger(t))
	require.Error(t, err)
	assert.ErrorContains(t, err, "payments")
	assert.ErrorContains(t, err, "cannot create upstream")

	useFakeGroup(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, RunSubscription(ctx, nil, mocksarama.NewFakeClient(),
		&schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host}, newTestLogger(t)))
}

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func captureClient(t *testing.T) *sarama.Config {
	t.Helper()
	var got sarama.Config
	old := newClient
	newClient = func(_ []string, cfg *sarama.Config) (sarama.Client, error) {
		got = *cfg
		return mocksarama.NewFakeClient(), nil
	}
	t.Cleanup(func() { newClient = old })
	return &got
}

func TestNewConsumerClient_ConfigHandling(t *testing.T) {
	conf := &schemas.KafkaInfo{Brokers: []string{"b:1"}, Version: "2.3.0"}
	cc := &schemas.ConsumerConfig{ClientId: "cid"}

	t.Run("missing file", func(t *testing.T) {
		old := consumerConfigPath
		consumerConfigPath = filepath.Join(t.TempDir(), "nope.yaml")
		t.Cleanup(func() { consumerConfigPath = old })
		_, err := NewConsumerClient(conf, cc)
		assert.ErrorContains(t, err, "unable to locate configuration file")
	})

	t.Run("unparsable file", func(t *testing.T) {
		old := consumerConfigPath
		consumerConfigPath = writeYAML(t, "net: [unclosed")
		t.Cleanup(func() { consumerConfigPath = old })
		_, err := NewConsumerClient(conf, cc)
		assert.ErrorContains(t, err, "unable to parse configuration")
	})

	t.Run("interval omitted keeps sarama default", func(t *testing.T) {
		old := consumerConfigPath
		consumerConfigPath = writeYAML(t, "consumer:\n  return:\n    errors: true\n")
		t.Cleanup(func() { consumerConfigPath = old })
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc)

		require.NoError(t, err)
		assert.Equal(t, sarama.NewConfig().Consumer.Offsets.AutoCommit.Interval, got.Consumer.Offsets.AutoCommit.Interval)
		assert.Contains(t, got.ClientID, "cid-")
		assert.Equal(t, sarama.V2_3_0_0, got.Version)
	})

	t.Run("interval set and TLS attached", func(t *testing.T) {
		old := consumerConfigPath
		consumerConfigPath = writeYAML(t, "OffsetsAutoCommitInterval: 2s\nnet:\n  tls:\n    enable: true\n")
		t.Cleanup(func() { consumerConfigPath = old })
		got := captureClient(t)
		withTLS := *conf
		withTLS.Version = "not-a-version"
		withTLS.TLSConf = nil

		_, err := NewConsumerClient(&withTLS, cc)

		require.NoError(t, err)
		assert.Equal(t, 2*time.Second, got.Consumer.Offsets.AutoCommit.Interval)
		assert.True(t, got.Net.TLS.Enable)
		assert.Equal(t, sarama.V2_7_0_0, got.Version)
	})

	t.Run("auto-commit cannot be disabled", func(t *testing.T) {
		old := consumerConfigPath
		consumerConfigPath = writeYAML(t, "consumer:\n  offsets:\n    autocommit:\n      enable: false\n")
		t.Cleanup(func() { consumerConfigPath = old })
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc)

		require.NoError(t, err)
		assert.True(t, got.Consumer.Offsets.AutoCommit.Enable)
	})
}

func TestNewConsumerClient_RealConstructorFailsAgainstDeadBroker(t *testing.T) {
	old := consumerConfigPath
	consumerConfigPath = writeYAML(t, "metadata:\n  retry:\n    max: 0\n")
	t.Cleanup(func() { consumerConfigPath = old })

	_, err := NewConsumerClient(&schemas.KafkaInfo{Brokers: []string{"127.0.0.1:1"}, Version: "2.3.0"}, &schemas.ConsumerConfig{})
	assert.Error(t, err)
}

func TestConsumeClaim_ConsumeEventHasNonZeroDuration(t *testing.T) {
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.SetClock(fakeclock.NewStepping(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10*time.Millisecond).Now)
	l.Run()
	l.RunAudit()

	f := newClaimFixture(t, 4)
	f.consumer.logger = l
	done := f.run()
	f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: 1, Value: []byte(`{}`)}
	select {
	case <-f.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("event not delivered")
	}
	f.cancel()
	waitDone(t, done)
	l.Shutdown()

	got := metricsfile.Durations(t, file, consts.EventConsume)
	require.Len(t, got, 1)
	for _, ms := range got {
		assert.Positive(t, ms, "consume event must be timed from the claim to the hand-over")
	}
}

// findKey returns the first value stored under key anywhere in a decoded JSON tree.
func findKey(v any, key string) (any, bool) {
	switch x := v.(type) {
	case map[string]any:
		if got, ok := x[key]; ok {
			return got, true
		}
		for _, child := range x {
			if got, ok := findKey(child, key); ok {
				return got, true
			}
		}
	case []any:
		for _, child := range x {
			if got, ok := findKey(child, key); ok {
				return got, true
			}
		}
	}
	return nil, false
}

func TestConsumeClaim_IngressAuditDurationCoversProcessing(t *testing.T) {
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.SetClock(fakeclock.NewStepping(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10*time.Millisecond).Now)
	l.Run()
	l.RunAudit()

	f := newClaimFixture(t, 4)
	f.consumer.logger = l
	done := f.run()
	f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: 1, Value: []byte(`{}`)}
	select {
	case <-f.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("event not delivered")
	}
	f.cancel()
	waitDone(t, done)
	l.Shutdown()

	raw, err := os.ReadFile(file + ".audit")
	require.NoError(t, err)
	var entry map[string]any
	require.NoError(t, json.Unmarshal(raw, &entry))
	dur, ok := findKey(entry, "duration")
	require.True(t, ok, "audit entry has no duration: %s", raw)

	nanos, _ := dur.(map[string]any)["nanos"].(float64)
	assert.GreaterOrEqual(t, int64(nanos), int64(10*time.Millisecond),
		"duration must span claim -> hand-over (>= the 10ms the consume event itself took), not ~0")
}

func TestRunSubscription_ShutdownAbandonsHungCallsWithinTheDrainWindowAndKeepsTheOffset(t *testing.T) {
	g := useFakeGroup(t)
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	conf := &schemas.ConsumerConfig{
		Topics: []string{"in"}, Group: "g",
		Service:     schemas.HostConfig{Name: u.Hostname(), Port: u.Port(), Path: "/predict", Timeout: 30 * time.Second},
		DrainWindow: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunSubscription(ctx, nil, mocksarama.NewFakeClient(), conf, newTestLogger(t))
	}()
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 7, Value: []byte(`{}`)})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream call never started")
	}

	began := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunSubscription did not stop")
	}

	assert.Less(t, time.Since(began), 3*time.Second, "bounded by the drain window, not by the 30s call timeout")
	assert.Empty(t, g.MarkedOffsets(), "the in-flight record must be redelivered, not marked")
}

func TestOffsetTracker_MarksOnlyInOrder(t *testing.T) {
	sess := &ctxSession{ctx: context.Background()}
	tr := &offsetTracker{session: sess}

	ack1 := tr.track(&sarama.ConsumerMessage{Offset: 1})
	ack2 := tr.track(&sarama.ConsumerMessage{Offset: 2})
	ack3 := tr.track(&sarama.ConsumerMessage{Offset: 3})

	ack3()
	ack2()
	assert.Empty(t, sess.Marked(), "nothing may be committed while offset 1 is in flight")

	ack1()
	assert.Equal(t, []int64{1, 2, 3}, sess.Marked())
}

func TestOffsetTracker_AckIsIdempotent(t *testing.T) {
	sess := &ctxSession{ctx: context.Background()}
	tr := &offsetTracker{session: sess}

	ack := tr.track(&sarama.ConsumerMessage{Offset: 7})
	ack()
	ack()

	assert.Equal(t, []int64{7}, sess.Marked())
}
