package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/natefinch/lumberjack.v2"

	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// Audit pipeline tuning: a lossless file stage, then a best-effort gRPC stage (baseline decision
// OBS-1). Variables so tests can shrink them.
var (
	auditQueueSize     = 4096            // entries waiting for the file writer; full means wait for the disk
	grpcQueueSize      = 4096            // entries waiting for the ALS sender; full means drop from gRPC only
	auditBatchSize     = 256             // entries per file write batch and per ALS stream
	auditSyncInterval  = time.Second     // fsync at least this often under load (and whenever idle)
	grpcStreamTimeout  = 3 * time.Second // per ALS stream
	grpcShutdownBudget = 3 * time.Second // for the entries still waiting for gRPC at shutdown
	grpcDropReport     = time.Minute     // at most one warning per interval about dropped deliveries
	rotateMaxSizeMB    = 100             // metric and audit files rotate at this size (MB)
)

// FileTransactionLogger is the concrete implementation of schemas.TransactionLogger
// that persists metrics to a local file and writes Envoy-compatible audit entries to a
// sibling `<file>.audit`, then delivers them to an ALS gRPC endpoint when one is configured.
type FileTransactionLogger struct {
	*schemas.FileTransactionLogger
	eventsAudit chan *accesslog.StreamAccessLogsMessage // file stage
	grpcAudit   chan *accesslog.StreamAccessLogsMessage // gRPC stage; nil without a client
	errorsAudit chan error
	mu          sync.Mutex
	auditPath   string
	auditSink   io.WriteCloser
	syncHandle  *os.File     // read-only handle used to fsync the current audit file
	syncAudit   func() error // fsyncs the audit file; a variable so tests can count calls
	grpcClient  accesslog.AccessLogServiceClient
	grpcConn    *grpc.ClientConn
	grpcDropped atomic.Int64
	grpcBase    context.Context // parent of every ALS stream; cancelled when the shutdown budget is spent
	grpcCancel  context.CancelFunc
	clock       func() time.Time // nil means time.Now

	stop         chan struct{} // closed by Shutdown: loops drain their queue and exit
	stopped      chan struct{} // closed once the metric loop and the audit file writer have drained
	wg           sync.WaitGroup
	grpcWG       sync.WaitGroup
	shutdownOnce sync.Once
}

// SetClock overrides the clock used for event start stamping, duration at close and audit
// timestamps; nil restores time.Now. Tests pass a fake clock to assert latencies.
func (l *FileTransactionLogger) SetClock(now func() time.Time) { l.clock = now }

func (l *FileTransactionLogger) now() time.Time {
	if l.clock == nil {
		return time.Now()
	}
	return l.clock()
}

// Option configures NewFileTransactionLogger.
type Option func(*options)

type options struct {
	maxBackups, maxAgeDays int
}

// WithRetention limits the backups of the metric and audit files: at most maxBackups of each and
// none older than maxAgeDays; 0 means no limit for that dimension. Without it every backup is kept.
func WithRetention(maxBackups, maxAgeDays int) Option {
	return func(o *options) { o.maxBackups, o.maxAgeDays = maxBackups, maxAgeDays }
}

// rotatingFile checks that name can be opened for appending (creating it when missing), makes it
// private to the service user (files of earlier versions were 0755, and lumberjack carries the
// mode into every rotated file), and returns a writer that rotates it by size into compressed
// backups. Backups are deleted only beyond the retention in o, which keeps all of them by default.
func rotatingFile(name string, o options) (io.WriteCloser, error) {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	if err := os.Chmod(name, 0o600); err != nil {
		return nil, err
	}
	return &lumberjack.Logger{
		Filename: name, MaxSize: rotateMaxSizeMB, Compress: true,
		MaxBackups: o.maxBackups, MaxAge: o.maxAgeDays,
	}, nil
}

// NewFileTransactionLogger opens the metrics and audit files and, when grpcEndpoint is set, dials
// the ALS endpoint (insecure); a dial failure leaves only the file. It keeps the deprecated
// grpc.DialContext: NewClient resolves targets via dns and is untested against real ALS endpoints.
func NewFileTransactionLogger(filename string, grpcEndpoint string, opts ...Option) (*FileTransactionLogger, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	sink, err := rotatingFile(filename, o)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log File '%s': %w", filename, err)
	}

	auditFilename := filename + ".audit"
	auditSink, err := rotatingFile(auditFilename, o)
	if err != nil {
		_ = sink.Close()
		return nil, fmt.Errorf("cannot open audit log File '%s': %w", auditFilename, err)
	}

	var grpcClient accesslog.AccessLogServiceClient
	var grpcConn *grpc.ClientConn

	if grpcEndpoint != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		conn, err := grpc.DialContext(ctx, grpcEndpoint, //nolint:staticcheck // SA1019, see the doc comment
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Warn().Err(err).Msgf("failed to connect to gRPC access log service at %s, continuing without gRPC", grpcEndpoint)
		} else {
			grpcConn = conn
			grpcClient = accesslog.NewAccessLogServiceClient(conn)
			log.Info().Msgf("connected to gRPC access log service at %s", grpcEndpoint)
		}
	}

	l := &FileTransactionLogger{
		FileTransactionLogger: &schemas.FileTransactionLogger{Sink: sink},
		eventsAudit:           make(chan *accesslog.StreamAccessLogsMessage, auditQueueSize),
		errorsAudit:           make(chan error, 1),
		auditPath:             auditFilename,
		auditSink:             auditSink,
		grpcClient:            grpcClient,
		grpcConn:              grpcConn,
		stop:                  make(chan struct{}),
		stopped:               make(chan struct{}),
	}
	l.syncAudit = l.fsyncAuditFile
	l.grpcBase, l.grpcCancel = context.WithCancel(context.Background())
	if grpcClient != nil {
		l.grpcAudit = make(chan *accesslog.StreamAccessLogsMessage, grpcQueueSize)
	}
	return l, nil
}

// Run starts the loop that writes Events to the metrics file until Shutdown.
func (l *FileTransactionLogger) Run() {
	events := make(chan schemas.Event, 16)
	l.Events = events

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for {
			select {
			case e := <-events:
				l.processEvent(e)
			case <-l.stop:
				for {
					select {
					case e := <-events:
						l.processEvent(e)
					default:
						return
					}
				}
			}
		}
	}()
}

func (l *FileTransactionLogger) processEvent(e schemas.Event) {
	latency := e.Duration.Milliseconds()
	log.Info().
		Str(consts.HeaderRequestIdKey, e.RequestId).
		Msgf(
			"event: %v, %s: %v, status: %v, latency: %v", consts.LogMapEventName[e.EventType], consts.HeaderRequestIdKey, e.RequestId, e.Status, latency,
		)

	l.metricToFile(e.RequestId, consts.MetricsSuccessMapName[e.EventType], strconv.Itoa(e.Success))
	l.metricToFile(e.RequestId, consts.MetricsFailureMapName[e.EventType], strconv.Itoa(e.Failure))
	l.metricToFile(e.RequestId, consts.MetricsRequestMapName[e.EventType], e.Status)
	l.metricToFile(e.RequestId, consts.MetricsDurationMapName[e.EventType], fmt.Sprintf("%v", latency))
}

// RunAudit starts the audit file writer and, with a gRPC client, the ALS sender, until Shutdown.
func (l *FileTransactionLogger) RunAudit() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.runAuditWriter()
	}()
	if l.grpcAudit != nil {
		l.grpcWG.Add(1)
		go func() {
			defer l.grpcWG.Done()
			l.runGRPCSender()
		}()
	}
}

// runAuditWriter is the lossless file stage: it writes queued entries in batches, fsyncs at least
// once per auditSyncInterval and whenever the queue runs empty, then offers each written entry to
// the gRPC stage. On stop it writes everything still queued, fsyncs and closes the gRPC stage.
func (l *FileTransactionLogger) runAuditWriter() {
	batch := make([]*accesslog.StreamAccessLogsMessage, 0, auditBatchSize)
	lastSync := time.Now()
	flush := func(force bool) {
		l.writeAudit(batch)
		if force || len(l.eventsAudit) == 0 || time.Since(lastSync) >= auditSyncInterval {
			if err := l.syncAudit(); err != nil {
				l.reportAuditError(fmt.Errorf("failed to sync audit file: %w", err))
			}
			lastSync = time.Now()
		}
		l.offerGRPC(batch)
		batch = batch[:0]
	}
	fill := func() {
		for len(batch) < auditBatchSize {
			select {
			case e := <-l.eventsAudit:
				batch = append(batch, e)
			default:
				return
			}
		}
	}

	for {
		select {
		case e := <-l.eventsAudit:
			batch = append(batch, e)
			fill()
			flush(false)
		case err := <-l.errorsAudit:
			log.Error().Err(err).Msg("audit processing error")
		case <-l.stop:
			for {
				fill()
				if len(batch) == 0 {
					break
				}
				flush(false)
			}
			if err := l.syncAudit(); err != nil {
				l.reportAuditError(fmt.Errorf("failed to sync audit file: %w", err))
			}
			if l.grpcAudit != nil {
				close(l.grpcAudit)
			}
			return
		}
	}
}

// writeAudit appends the entries to the audit file as JSON lines, in order. Each line is its own
// write: the rotating file refuses a single write larger than its size limit, so a batch of large
// entries must not fail as a whole.
func (l *FileTransactionLogger) writeAudit(batch []*accesslog.StreamAccessLogsMessage) {
	for _, msg := range batch {
		data, err := json.Marshal(msg)
		if err != nil {
			l.reportAuditError(fmt.Errorf("failed to marshal StreamAccessLogsMessage: %w", err))
			continue
		}
		if _, err := l.auditSink.Write(append(data, '\n')); err != nil {
			l.reportAuditError(fmt.Errorf("failed to write audit event to file: %w", err))
		}
	}
}

// fsyncAuditFile flushes the current audit file to disk through a read-only handle, reopened
// whenever lumberjack has rotated the file under it.
func (l *FileTransactionLogger) fsyncAuditFile() error {
	cur, err := os.Stat(l.auditPath)
	if err != nil {
		return err
	}
	if l.syncHandle != nil {
		if held, err := l.syncHandle.Stat(); err != nil || !os.SameFile(cur, held) {
			_ = l.syncHandle.Close()
			l.syncHandle = nil
		}
	}
	if l.syncHandle == nil {
		if l.syncHandle, err = os.Open(l.auditPath); err != nil {
			return err
		}
	}
	return l.syncHandle.Sync()
}

// offerGRPC hands written entries to the gRPC stage without ever waiting: on a full queue the
// entry is counted as not delivered (it is already in the audit file).
func (l *FileTransactionLogger) offerGRPC(batch []*accesslog.StreamAccessLogsMessage) {
	if l.grpcAudit == nil {
		return
	}
	for _, msg := range batch {
		select {
		case l.grpcAudit <- msg:
		default:
			l.grpcDropped.Add(1)
		}
	}
}

// runGRPCSender is the best-effort gRPC stage: it ships queued entries in batches, one ALS stream
// per batch, and reports dropped deliveries at most once per grpcDropReport. It returns when the
// file writer has closed the queue and everything left has been attempted.
func (l *FileTransactionLogger) runGRPCSender() {
	ticker := time.NewTicker(grpcDropReport)
	defer ticker.Stop()
	defer l.reportGRPCDrops()

	batch := make([]*accesslog.StreamAccessLogsMessage, 0, auditBatchSize)
	for {
		select {
		case msg, ok := <-l.grpcAudit:
			if !ok {
				return
			}
			batch = append(batch[:0], msg)
		fill:
			for len(batch) < auditBatchSize {
				select {
				case next, ok := <-l.grpcAudit:
					if !ok {
						break fill
					}
					batch = append(batch, next)
				default:
					break fill
				}
			}
			l.sendGRPC(batch)
		case <-ticker.C:
			l.reportGRPCDrops()
		}
	}
}

func (l *FileTransactionLogger) reportGRPCDrops() {
	if n := l.grpcDropped.Swap(0); n > 0 {
		log.Warn().Msgf("%d audit entries were not delivered to the ALS endpoint (the audit file has them)", n)
	}
}

// sendGRPC delivers one batch over a single ALS stream.
func (l *FileTransactionLogger) sendGRPC(batch []*accesslog.StreamAccessLogsMessage) {
	ctx, cancel := context.WithTimeout(l.grpcBase, grpcStreamTimeout)
	defer cancel()

	stream, err := l.grpcClient.StreamAccessLogs(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to create stream for access logs")
		return
	}
	for _, msg := range batch {
		if err := stream.Send(msg); err != nil {
			log.Error().Err(err).Msg("access log failed to send access log message")
			return
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		if err != io.EOF {
			log.Warn().Err(err).Msg("access log error receiving response (ignored)")
		}
	} else if resp != nil {
		log.Debug().Msgf("access log received response successfully: %v", resp)
	}
}

// Shutdown stops the processing loops after they have written every queued
// event, gives the entries still waiting for gRPC one bounded attempt, then
// closes files and the gRPC connection. It is safe to call more than once.
// Events submitted afterwards are dropped instead of blocking.
func (l *FileTransactionLogger) Shutdown() {
	l.shutdownOnce.Do(func() {
		close(l.stop)
		l.wg.Wait()
		close(l.stopped)

		sent := make(chan struct{})
		go func() { l.grpcWG.Wait(); close(sent) }()
		select {
		case <-sent:
		case <-time.After(grpcShutdownBudget):
			l.grpcCancel()
			<-sent
		}
		l.grpcCancel()
		l.CloseAll()
	})
}

// CloseAudit queues an audit entry for the audit file. It waits only for the file writer, never
// for the ALS endpoint, and never drops an entry; after Shutdown it returns at once.
func (l *FileTransactionLogger) CloseAudit(e *accesslog.StreamAccessLogsMessage) {
	select {
	case l.eventsAudit <- e:
	case <-l.stopped:
	}
}

// Start stamps the event's own start time; Close measures from it.
func (l *FileTransactionLogger) Start(e *schemas.Event) {
	e.Started = l.now()
}

// Close finishes an event: its duration is the time from its own Start to now
// (0 if it was never started), taken here so that time spent waiting in the
// metrics queue is not counted. Events closed after Shutdown are dropped.
func (l *FileTransactionLogger) Close(e *schemas.Event) {
	var duration time.Duration
	if e.Started.IsZero() {
		log.Debug().Str(consts.HeaderRequestIdKey, e.RequestId).Msg("event closed without Start, duration 0")
	} else if duration = l.now().Sub(e.Started); duration < 0 {
		duration = 0
	}

	ev := schemas.Event{
		EventType: e.EventType,
		RequestId: e.RequestId,
		Status:    e.Status,
		Started:   e.Started,
		Duration:  duration,
		Success:   e.Success,
		Failure:   e.Failure,
	}

	select {
	case l.Events <- ev:
	case <-l.stopped:
	}
}

// metricToFile writes one metric as a JSON line to the metrics file.
func (l *FileTransactionLogger) metricToFile(requestId string, metricName string, metricValue string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	fileMetric := zerolog.New(l.Sink)
	fileMetric.Log().
		Str(consts.FieldMetricRquid, requestId).
		Str("metric_name", metricName).
		Str("metric_value", metricValue).
		Send()
}

// reportAuditError never blocks: it is called from the very goroutine that
// drains errorsAudit, so a blocking send on the full buffer would deadlock it.
func (l *FileTransactionLogger) reportAuditError(err error) {
	select {
	case l.errorsAudit <- err:
	default:
		log.Error().Err(err).Msg("audit error (error queue full)")
	}
}

// CloseAll closes every resource, reporting (but not stopping at) individual failures.
func (l *FileTransactionLogger) CloseAll() {
	if err := l.Sink.Close(); err != nil {
		log.Error().Msgf("error closing metric file: %v", err)
	}
	if err := l.auditSink.Close(); err != nil {
		log.Error().Msgf("error closing file '%s': %v", l.auditPath, err)
	}
	if l.syncHandle != nil {
		_ = l.syncHandle.Close()
	}
	if l.grpcConn != nil {
		if err := l.grpcConn.Close(); err != nil {
			log.Error().Msgf("error closing grpc conn: %v", err)
		}
	}
}
