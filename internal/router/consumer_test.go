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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
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
	pause := NewPause()
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunSubscription(ctx, pause, mocksarama.NewFakeClient(), conf, nil, newTestLogger(t), nil)
	}()

	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 3, Value: []byte(`{"q":1}`)})
	require.Eventually(t, func() bool { return len(g.MarkedOffsets()) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []int64{3}, g.MarkedOffsets())

	pause.Toggle()
	assert.Equal(t, 1, g.PauseCount(), "the subscription's group follows the shared pause")
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 4, Value: []byte(`{"q":2}`)})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, []int64{3}, g.MarkedOffsets(), "no record is handed out while paused")

	pause.Toggle()
	assert.Equal(t, 1, g.ResumeCount())
	require.Eventually(t, func() bool { return len(g.MarkedOffsets()) == 2 }, 5*time.Second, 10*time.Millisecond,
		"the held record is processed after resume")

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
		RunSubscription(context.Background(), nil, mocksarama.NewFakeClient(), conf, nil, newTestLogger(t), nil)
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
		RunSubscription(ctx, nil, mocksarama.NewFakeClient(), conf, nil, newTestLogger(t), nil)
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
		&schemas.ConsumerConfig{Topics: []string{"orders"}, Group: "g", Service: host}, nil, newTestLogger(t), nil)
	newConsumerGroup = old
	require.Error(t, err)
	assert.ErrorContains(t, err, "orders")
	assert.ErrorContains(t, err, "cannot create consumer group")
	assert.ErrorContains(t, err, "no group")

	badTarget := schemas.HostConfig{Name: "%zz", Port: "1", Protocol: schemas.ProtocolGRPC}
	err = RunSubscription(context.Background(), nil, mocksarama.NewFakeClient(),
		&schemas.ConsumerConfig{Topics: []string{"payments"}, Group: "g", Service: badTarget}, nil, newTestLogger(t), nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "payments")
	assert.ErrorContains(t, err, "cannot create upstream")

	useFakeGroup(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, RunSubscription(ctx, nil, mocksarama.NewFakeClient(),
		&schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host}, nil, newTestLogger(t), nil))
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

// consumerTuning loads content as the consumer tuning file.
func consumerTuning(t *testing.T, content string) *Tuning {
	t.Helper()
	useConsumerFile(t, content)
	tuning, err := LoadConsumerTuning()
	require.NoError(t, err)
	return tuning
}

func TestNewConsumerClient_ConfigHandling(t *testing.T) {
	conf := &schemas.KafkaInfo{Brokers: []string{"b:1"}, Version: "2.3.0"}
	cc := &schemas.ConsumerConfig{ClientId: "cid"}

	t.Run("interval omitted keeps sarama default", func(t *testing.T) {
		tuning := consumerTuning(t, "consumer:\n  return:\n    errors: true\n")
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc, tuning)

		require.NoError(t, err)
		assert.Equal(t, sarama.NewConfig().Consumer.Offsets.AutoCommit.Interval, got.Consumer.Offsets.AutoCommit.Interval)
		assert.Contains(t, got.ClientID, "cid-")
		assert.Equal(t, sarama.V2_3_0_0, got.Version)
	})

	t.Run("interval set and TLS attached", func(t *testing.T) {
		tuning := consumerTuning(t, "OffsetsAutoCommitInterval: 2s\nnet:\n  tls:\n    enable: true\n")
		got := captureClient(t)
		withTLS := *conf
		withTLS.Version = "not-a-version"
		withTLS.TLSConf = nil

		_, err := NewConsumerClient(&withTLS, cc, tuning)

		require.NoError(t, err)
		assert.Equal(t, 2*time.Second, got.Consumer.Offsets.AutoCommit.Interval)
		assert.True(t, got.Net.TLS.Enable)
		assert.Equal(t, sarama.V2_7_0_0, got.Version)
	})

	t.Run("auto-commit cannot be disabled", func(t *testing.T) {
		tuning := consumerTuning(t, "consumer:\n  offsets:\n    autocommit:\n      enable: false\n")
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc, tuning)

		require.NoError(t, err)
		assert.True(t, got.Consumer.Offsets.AutoCommit.Enable)
	})

	t.Run("code defaults without tuning", func(t *testing.T) {
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc, nil)

		require.NoError(t, err)
		assert.Equal(t, 60*time.Second, got.Consumer.Group.Session.Timeout)
		assert.Equal(t, 10*time.Second, got.Consumer.Group.Heartbeat.Interval)
		assert.Equal(t, 10*time.Second, got.Consumer.MaxProcessingTime)
		require.Len(t, got.Consumer.Group.Rebalance.GroupStrategies, 1)
		assert.Equal(t, "range", got.Consumer.Group.Rebalance.GroupStrategies[0].Name())
		assert.True(t, got.Consumer.Offsets.AutoCommit.Enable)
	})

	t.Run("session timeout overrides the default", func(t *testing.T) {
		tuning := consumerTuning(t, "consumer:\n  group:\n    session:\n      timeout: 20s\n")
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc, tuning)

		require.NoError(t, err)
		assert.Equal(t, 20*time.Second, got.Consumer.Group.Session.Timeout)
		assert.Equal(t, 10*time.Second, got.Consumer.Group.Heartbeat.Interval, "other defaults are kept")
	})

	t.Run("strategy reaches the group config", func(t *testing.T) {
		tuning := consumerTuning(t, "consumer:\n  group:\n    rebalance:\n      strategy: cooperative-sticky\n")
		got := captureClient(t)

		_, err := NewConsumerClient(conf, cc, tuning)

		require.NoError(t, err)
		require.Len(t, got.Consumer.Group.Rebalance.GroupStrategies, 1)
		assert.Equal(t, "cooperative-sticky", got.Consumer.Group.Rebalance.GroupStrategies[0].Name())
	})
}

func TestNewConsumerClient_RealConstructorFailsAgainstDeadBroker(t *testing.T) {
	tuning := consumerTuning(t, "metadata:\n  retry:\n    max: 0\n")

	_, err := NewConsumerClient(&schemas.KafkaInfo{Brokers: []string{"127.0.0.1:1"}, Version: "2.3.0"}, &schemas.ConsumerConfig{}, tuning)
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
		RunSubscription(ctx, nil, mocksarama.NewFakeClient(), conf, nil, newTestLogger(t), nil)
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

	ack1, _ := tr.track(&sarama.ConsumerMessage{Offset: 1})
	ack2, _ := tr.track(&sarama.ConsumerMessage{Offset: 2})
	ack3, _ := tr.track(&sarama.ConsumerMessage{Offset: 3})

	ack3()
	ack2()
	assert.Empty(t, sess.Marked(), "nothing may be committed while offset 1 is in flight")

	ack1()
	assert.Equal(t, []int64{1, 2, 3}, sess.Marked())
}

func TestOffsetTracker_AckIsIdempotent(t *testing.T) {
	sess := &ctxSession{ctx: context.Background()}
	tr := &offsetTracker{session: sess}

	ack, _ := tr.track(&sarama.ConsumerMessage{Offset: 7})
	ack()
	ack()

	assert.Equal(t, []int64{7}, sess.Marked())
}

// The ack and the abandon of one record share one outcome: whichever comes first wins.
func TestOffsetTracker_AckAndAbandonFireOnce(t *testing.T) {
	sess := &ctxSession{ctx: context.Background()}
	tr := &offsetTracker{session: sess}

	ack, abandon := tr.track(&sarama.ConsumerMessage{Offset: 1})
	ack()
	abandon()
	assert.Equal(t, []int64{1}, sess.Marked())
	assert.Zero(t, tr.waitSettled(0))

	ack2, abandon2 := tr.track(&sarama.ConsumerMessage{Offset: 2})
	abandon2()
	ack2()
	assert.Equal(t, []int64{1}, sess.Marked(), "an abandoned record is never marked")
	assert.Zero(t, tr.waitSettled(0), "an abandoned record no longer counts as outstanding")
}

// An abandoned record stops marking in front of it: later records stay unmarked, so the
// abandoned one is redelivered together with everything after it.
func TestOffsetTracker_AbandonedRecordBlocksLaterMarks(t *testing.T) {
	sess := &ctxSession{ctx: context.Background()}
	tr := &offsetTracker{session: sess}

	_, abandon1 := tr.track(&sarama.ConsumerMessage{Offset: 1})
	ack2, _ := tr.track(&sarama.ConsumerMessage{Offset: 2})
	abandon1()
	ack2()

	assert.Empty(t, sess.Marked())
}

func TestOffsetTracker_WaitSettled(t *testing.T) {
	tr := &offsetTracker{session: &ctxSession{ctx: context.Background()}}
	ack, _ := tr.track(&sarama.ConsumerMessage{Offset: 1})
	_, _ = tr.track(&sarama.ConsumerMessage{Offset: 2})

	time.AfterFunc(50*time.Millisecond, ack)
	began := time.Now()
	assert.Equal(t, 1, tr.waitSettled(300*time.Millisecond), "one record never settles")
	assert.GreaterOrEqual(t, time.Since(began), 300*time.Millisecond, "the bound is waited out")

	tr2 := &offsetTracker{session: &ctxSession{ctx: context.Background()}}
	ack3, _ := tr2.track(&sarama.ConsumerMessage{Offset: 3})
	time.AfterFunc(50*time.Millisecond, ack3)
	began = time.Now()
	assert.Zero(t, tr2.waitSettled(5*time.Second))
	assert.Less(t, time.Since(began), 2*time.Second, "the wait ends as soon as everything settled")
}

// blockingUpstream answers each call only when release is closed (or the call is cancelled), and
// reports every call start on started.
func blockingUpstream(t *testing.T) (host schemas.HostConfig, started chan struct{}, release chan struct{}) {
	t.Helper()
	started, release = make(chan struct{}, 64), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		select {
		case <-release:
			_, _ = w.Write([]byte(`{}`))
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	return schemas.HostConfig{Name: u.Hostname(), Port: u.Port(), Path: "/predict", Timeout: 30 * time.Second}, started, release
}

func waitStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream call never started")
	}
}

// A call that finishes inside the drain window on shutdown must have its offset committed before
// the session is released, so the record is not redelivered.
func TestRunSubscription_CallFinishingInTheDrainWindowIsMarkedBeforeRelease(t *testing.T) {
	g := useFakeGroup(t)
	host, started, release := blockingUpstream(t)
	conf := &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host, DrainWindow: 3 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunSubscription(ctx, nil, mocksarama.NewFakeClient(), conf, nil, newTestLogger(t), nil)
	}()
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 9, Value: []byte(`{}`)})
	waitStarted(t, started)

	cancel()
	time.AfterFunc(200*time.Millisecond, func() { close(release) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RunSubscription did not stop")
	}

	assert.Equal(t, []int64{9}, g.MarkedOffsets(), "the record finished in the drain window and must not be redelivered")
}

// runSubscriptionInBackground starts RunSubscription and returns a stop function that cancels it
// and waits for it to return.
func runSubscriptionInBackground(t *testing.T, conf *schemas.ConsumerConfig) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunSubscription(ctx, nil, mocksarama.NewFakeClient(), conf, nil, newTestLogger(t), nil)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("RunSubscription did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// A call in flight when a rebalance ends the session, and finishing within the bound, is marked
// before the session is released, so the partition's next owner does not get it again.
func TestRunSubscription_InFlightCallDuringRebalanceIsMarkedBeforeRelease(t *testing.T) {
	g := useFakeGroup(t)
	host, started, release := blockingUpstream(t)
	runSubscriptionInBackground(t, &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host, DrainWindow: 3 * time.Second})
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 4, Value: []byte(`{}`)})
	waitStarted(t, started)

	g.EndSession()
	time.AfterFunc(200*time.Millisecond, func() { close(release) })

	require.Eventually(t, func() bool { return len(g.MarkedOffsets()) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []int64{4}, g.MarkedOffsets())
}

// A call that does not finish within the bound does not hold the rebalance: the claim returns
// after the bound with a warning, and the record stays unmarked for redelivery.
func TestRunSubscription_RebalanceWaitIsBounded(t *testing.T) {
	g := useFakeGroup(t)
	logs := captureWarnings(t)
	host, started, release := blockingUpstream(t)
	t.Cleanup(func() { close(release) })
	runSubscriptionInBackground(t, &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host, DrainWindow: 300 * time.Millisecond})
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 5, Value: []byte(`{}`)})
	waitStarted(t, started)

	began := time.Now()
	g.EndSession()

	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "still in flight") }, 5*time.Second, 10*time.Millisecond)
	assert.Less(t, time.Since(began), 2*time.Second, "bounded by the drain window, not by the 30s call timeout")
	assert.Contains(t, logs.String(), "1 records still in flight")
	assert.Empty(t, g.MarkedOffsets())
}

// On a rebalance, records of the ended session that wait behind a busy slot are not sent to the
// upstream, and being abandoned they do not hold the claim: it returns as soon as the in-flight
// call settles, long before the bound.
func TestRunSubscription_BufferedRecordsOfARevokedPartitionAreNotSent(t *testing.T) {
	g := useFakeGroup(t)
	host, started, release := blockingUpstream(t)
	host.MaxConn = 1
	runSubscriptionInBackground(t, &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host, DrainWindow: 5 * time.Second})
	for i := range 21 {
		g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: int64(i), Value: []byte(`{}`)})
	}
	waitStarted(t, started)
	time.Sleep(200 * time.Millisecond) // let the claim hand all 21 records to the pipeline

	began := time.Now()
	g.EndSession()
	time.AfterFunc(100*time.Millisecond, func() { close(release) })

	require.Eventually(t, func() bool { return g.Sessions() == 2 }, 4*time.Second, 10*time.Millisecond)
	assert.Less(t, time.Since(began), 2*time.Second, "abandoned records must not hold the claim until the bound")
	time.Sleep(200 * time.Millisecond)
	assert.Len(t, started, 0, "none of the buffered records reached the upstream")
	assert.Equal(t, []int64{0}, g.MarkedOffsets(), "only the in-flight record settled")
}

// On shutdown the claim's wait and the dispatcher's drain share one deadline: a hung call is
// given one drain window in total, not one per stage.
func TestRunSubscription_ShutdownWaitsOneDrainWindowInTotal(t *testing.T) {
	g := useFakeGroup(t)
	host, started, release := blockingUpstream(t)
	t.Cleanup(func() { close(release) })
	stop := runSubscriptionInBackground(t, &schemas.ConsumerConfig{Topics: []string{"in"}, Group: "g", Service: host, DrainWindow: 400 * time.Millisecond})
	g.QueueMessage(&sarama.ConsumerMessage{Topic: "in", Offset: 1, Value: []byte(`{}`)})
	waitStarted(t, started)

	began := time.Now()
	stop()
	elapsed := time.Since(began)

	assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond, "the in-flight call gets the drain window")
	assert.Less(t, elapsed, 700*time.Millisecond, "one drain window in total, not two")
	assert.Empty(t, g.MarkedOffsets())
}

// Records held behind an unsettled one count as uncommitted until they are marked or their claim
// ends; then the pipeline's count returns to zero.
func TestConsumeClaim_ReportsUncommittedRecords(t *testing.T) {
	f := newClaimFixture(t, 64)
	state := transport.NewPipelineState()
	f.consumer.state = state
	done := f.run()

	for i := range 41 {
		f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: int64(i), Value: []byte(`{}`)}
	}
	events := make([]*schemas.ConsumerEvent, 0, 41)
	for range 41 {
		events = append(events, <-f.ch)
	}
	for _, ev := range events[1:] {
		ev.Ack()
	}

	assert.Equal(t, int64(41), state.Snapshot().Uncommitted, "40 settled records wait behind the first one")
	assert.Empty(t, f.session.Marked())

	f.cancel()
	require.NoError(t, waitDone(t, done))
	assert.Zero(t, state.Snapshot().Uncommitted, "an ended claim no longer counts its records")

	events[0].Ack()
	assert.Zero(t, state.Snapshot().Uncommitted, "a late ack does not count twice")
}

func TestOffsetTracker_UncommittedFollowsMarks(t *testing.T) {
	state := transport.NewPipelineState()
	tr := &offsetTracker{session: &ctxSession{ctx: context.Background()}, state: state}

	ack1, _ := tr.track(&sarama.ConsumerMessage{Offset: 1})
	ack2, _ := tr.track(&sarama.ConsumerMessage{Offset: 2})
	assert.Equal(t, int64(2), state.Snapshot().Uncommitted)

	ack2()
	assert.Equal(t, int64(2), state.Snapshot().Uncommitted, "offset 2 waits for offset 1")
	ack1()
	assert.Zero(t, state.Snapshot().Uncommitted)
}

// A claim that starts while the process is paused pauses its partition and hands out nothing
// until resume; then the held record and the next one are handed out in order.
func TestConsumeClaim_PausedClaimHoldsRecordsUntilResume(t *testing.T) {
	f := newClaimFixture(t, 4)
	g := mocksarama.NewFakeConsumerGroup(nil)
	pause := NewPause()
	pause.Toggle()
	f.consumer.pause, f.consumer.group = pause, g
	done := f.run()

	f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: 1, Value: []byte(`{}`)}
	f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: 2, Value: []byte(`{}`)}
	require.Eventually(t, func() bool { return g.PauseCount() == 1 }, time.Second, 5*time.Millisecond,
		"the partition is paused at claim start")
	select {
	case <-f.ch:
		t.Fatal("no record may be handed out while paused")
	case <-time.After(100 * time.Millisecond):
	}

	pause.Toggle()
	first, second := <-f.ch, <-f.ch
	assert.Equal(t, "in", first.Values.Topic)
	first.Ack()
	second.Ack()
	assert.Equal(t, []int64{1, 2}, f.session.Marked(), "records are handed out in order after resume")

	f.cancel()
	require.NoError(t, waitDone(t, done))
}

// A session that ends while a record waits for resume returns without tracking it, so it is
// redelivered and nothing waits for its settlement.
func TestConsumeClaim_SessionEndDuringPauseLeavesTheRecordUntracked(t *testing.T) {
	f := newClaimFixture(t, 4)
	state := transport.NewPipelineState()
	pause := NewPause()
	pause.Toggle()
	f.consumer.pause, f.consumer.state = pause, state
	done := f.run()

	f.claim.msgs <- &sarama.ConsumerMessage{Topic: "in", Offset: 1, Value: []byte(`{}`)}
	time.Sleep(50 * time.Millisecond)
	f.cancel()

	require.NoError(t, waitDone(t, done))
	assert.Empty(t, f.ch, "the record was not handed out")
	assert.Empty(t, f.session.Marked())
	assert.Zero(t, state.Snapshot().Uncommitted, "the record was never tracked")
}
