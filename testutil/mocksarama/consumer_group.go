package mocksarama

// A hand-rolled fake of sarama's Client, ConsumerGroup, session and claim for unit tests of the
// consume orchestration. It does not model rebalancing, commits or heartbeats: behaviour against
// a real broker still needs an integration test.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
)

// ErrFakeClosed is returned by Consume after Close.
var ErrFakeClosed = errors.New("mocksarama: fake consumer group closed")

// errFakeUnimplemented is returned by the Client methods the fake does not model.
var errFakeUnimplemented = errors.New("mocksarama: fake client operation not implemented")

// FakeClient implements sarama.Client with just a topic/partition topology; other methods
// return errFakeUnimplemented.
type FakeClient struct {
	mu         sync.Mutex
	closed     int32 // atomic flag
	topics     []string
	partitions map[string][]int32
}

// NewFakeClient returns an empty client; callers may populate topology via AddTopicPartition.
func NewFakeClient() *FakeClient {
	return &FakeClient{
		topics:     []string{},
		partitions: map[string][]int32{},
	}
}

// AddTopicPartition registers one partition for a topic.
func (c *FakeClient) AddTopicPartition(topic string, partition int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := false
	for _, t := range c.topics {
		if t == topic {
			seen = true
			break
		}
	}
	if !seen {
		c.topics = append(c.topics, topic)
	}
	c.partitions[topic] = append(c.partitions[topic], partition)
}

// Config returns a fresh default sarama.Config.
func (c *FakeClient) Config() *sarama.Config { return sarama.NewConfig() }

func (c *FakeClient) Controller() (*sarama.Broker, error)        { return nil, errFakeUnimplemented }
func (c *FakeClient) RefreshController() (*sarama.Broker, error) { return nil, errFakeUnimplemented }
func (c *FakeClient) Brokers() []*sarama.Broker                  { return nil }
func (c *FakeClient) Broker(int32) (*sarama.Broker, error)       { return nil, errFakeUnimplemented }

func (c *FakeClient) Topics() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.topics))
	copy(out, c.topics)
	return out, nil
}

func (c *FakeClient) Partitions(string) ([]int32, error)           { return nil, errFakeUnimplemented }
func (c *FakeClient) WritablePartitions(string) ([]int32, error)   { return nil, errFakeUnimplemented }
func (c *FakeClient) Leader(string, int32) (*sarama.Broker, error) { return nil, errFakeUnimplemented }
func (c *FakeClient) LeaderAndEpoch(string, int32) (*sarama.Broker, int32, error) {
	return nil, 0, errFakeUnimplemented
}
func (c *FakeClient) Replicas(string, int32) ([]int32, error)       { return nil, errFakeUnimplemented }
func (c *FakeClient) InSyncReplicas(string, int32) ([]int32, error) { return nil, errFakeUnimplemented }
func (c *FakeClient) OfflineReplicas(string, int32) ([]int32, error) {
	return nil, errFakeUnimplemented
}
func (c *FakeClient) RefreshBrokers([]string) error                 { return errFakeUnimplemented }
func (c *FakeClient) RefreshMetadata(...string) error               { return errFakeUnimplemented }
func (c *FakeClient) GetOffset(string, int32, int64) (int64, error) { return 0, errFakeUnimplemented }
func (c *FakeClient) Coordinator(string) (*sarama.Broker, error)    { return nil, errFakeUnimplemented }
func (c *FakeClient) RefreshCoordinator(string) error               { return errFakeUnimplemented }
func (c *FakeClient) TransactionCoordinator(string) (*sarama.Broker, error) {
	return nil, errFakeUnimplemented
}
func (c *FakeClient) RefreshTransactionCoordinator(string) error { return errFakeUnimplemented }
func (c *FakeClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	return nil, errFakeUnimplemented
}
func (c *FakeClient) LeastLoadedBroker() *sarama.Broker       { return nil }
func (c *FakeClient) PartitionNotReadable(string, int32) bool { return true }

func (c *FakeClient) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	return nil
}

// Closed reports whether Close() has been called.
func (c *FakeClient) Closed() bool { return atomic.LoadInt32(&c.closed) == 1 }

// FakeConsumerGroup feeds its claims with the messages tests inject via QueueMessage.
type FakeConsumerGroup struct {
	client     sarama.Client
	handler    sarama.ConsumerGroupHandler
	topics     []string
	inbox      chan *sarama.ConsumerMessage
	errs       chan error
	paused     int32
	resumed    int32
	cancelOnce sync.Once
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closeMu    sync.Mutex
	closed     bool
	markMu     sync.Mutex
	marked     []int64
}

// NewFakeConsumerGroup returns a fake group over client.
func NewFakeConsumerGroup(client sarama.Client) *FakeConsumerGroup {
	return &FakeConsumerGroup{
		client: client,
		inbox:  make(chan *sarama.ConsumerMessage, 1024),
		errs:   make(chan error, 16),
	}
}

// QueueMessage enqueues a message that the next Recv on Claim.Messages() yields.
func (g *FakeConsumerGroup) QueueMessage(m *sarama.ConsumerMessage) { g.inbox <- m }

// QueueError buffers e; the fake never delivers it (Errors returns nil).
func (g *FakeConsumerGroup) QueueError(e error) { g.errs <- e }

// PauseCount / ResumeCount expose observed toggle frequencies for assertions.
func (g *FakeConsumerGroup) PauseCount() int  { return int(atomic.LoadInt32(&g.paused)) }
func (g *FakeConsumerGroup) ResumeCount() int { return int(atomic.LoadInt32(&g.resumed)) }

// Consume runs handler.ConsumeClaim for every topic over the shared inbox and returns once all
// claims end or ctx (or Close) cancels the session.
func (g *FakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if g.isClosed() {
		return ErrFakeClosed
	}
	g.handler = handler
	g.topics = topics
	runCtx, cancel := context.WithCancel(ctx)
	g.cancelOnce.Do(func() { g.cancel = cancel })

	sess := &fakeSession{ctx: runCtx, group: g}
	if err := handler.Setup(sess); err != nil {
		cancel()
		return err
	}

	done := make(chan struct{})
	for _, topic := range topics {
		top := topic
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			_ = handler.ConsumeClaim(sess, &fakeClaim{
				topic: top,
				msgs:  g.inbox,
			})
		}()
	}
	go func() {
		g.wg.Wait()
		close(done)
	}()

	var cleanupErr error
	select {
	case <-done:
	case <-runCtx.Done():
	}

	if err := handler.Cleanup(sess); err != nil {
		cleanupErr = err
	}
	cancel()
	if cleanupErr != nil {
		return cleanupErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Errors returns nil: the fake reports no asynchronous errors.
func (g *FakeConsumerGroup) Errors() <-chan error { return nil }

// Close terminates an in-flight Consume by cancelling its derived ctx and waiting
// for spawned goroutines to exit.
func (g *FakeConsumerGroup) Close() error {
	g.closeMu.Lock()
	if !g.closed {
		g.closed = true
		if g.cancel != nil {
			g.cancel()
		}
	}
	g.closeMu.Unlock()
	g.wg.Wait()
	return nil
}

func (g *FakeConsumerGroup) isClosed() bool {
	g.closeMu.Lock()
	defer g.closeMu.Unlock()
	return g.closed
}

func (g *FakeConsumerGroup) Pause(map[string][]int32)  { atomic.AddInt32(&g.paused, 1) }
func (g *FakeConsumerGroup) Resume(map[string][]int32) { atomic.AddInt32(&g.resumed, 1) }
func (g *FakeConsumerGroup) PauseAll()                 { atomic.AddInt32(&g.paused, 1) }
func (g *FakeConsumerGroup) ResumeAll()                { atomic.AddInt32(&g.resumed, 1) }

// fakeSession is the ConsumerGroupSession handed to handlers: its context is
// the one derived from Consume's ctx (so handlers can read values the caller
// attached and observe cancellation) and every MarkMessage is recorded.
type fakeSession struct {
	ctx   context.Context
	group *FakeConsumerGroup
}

func (*fakeSession) Claims() map[string][]int32               { return nil }
func (*fakeSession) MemberID() string                         { return "fake-member" }
func (*fakeSession) GenerationID() int32                      { return 0 }
func (*fakeSession) MarkOffset(string, int32, int64, string)  {}
func (*fakeSession) Commit()                                  {}
func (*fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) Context() context.Context               { return s.ctx }
func (s *fakeSession) MarkMessage(m *sarama.ConsumerMessage, _ string) {
	s.group.markMu.Lock()
	defer s.group.markMu.Unlock()
	s.group.marked = append(s.group.marked, m.Offset)
}

// MarkedOffsets returns the offsets handlers marked, in order.
func (g *FakeConsumerGroup) MarkedOffsets() []int64 {
	g.markMu.Lock()
	defer g.markMu.Unlock()
	return append([]int64(nil), g.marked...)
}

// fakeClaim delivers the parent group's inbox as one partition of topic.
type fakeClaim struct {
	topic string
	msgs  <-chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return c.topic }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return -1 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }

var _ sarama.Client = (*FakeClient)(nil)
