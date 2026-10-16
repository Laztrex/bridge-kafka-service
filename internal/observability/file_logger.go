package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// FileTransactionLogger is the concrete implementation of schemas.TransactionLogger
// that persists metrics to a local file and streams Envoy-compatible audit
// entries either to gRPC or to a sibling `<file>.audit` sink as fallback.
type FileTransactionLogger struct {
	*schemas.FileTransactionLogger
	eventsAudit chan *accesslog.StreamAccessLogsMessage
	errorsAudit chan error
	mu          sync.Mutex
	auditFile   *os.File
	grpcClient  accesslog.AccessLogServiceClient
	grpcConn    *grpc.ClientConn
	clock       func() time.Time // nil means time.Now

	stop         chan struct{} // closed by Shutdown: loops drain their queue and exit
	stopped      chan struct{} // closed once both loops have drained
	wg           sync.WaitGroup
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

// NewFileTransactionLogger opens the metrics file and, when grpcEndpoint is set, dials the ALS
// endpoint (insecure); a dial failure leaves only the file fallback. It keeps the deprecated
// grpc.DialContext: NewClient resolves targets via dns and is untested against real ALS endpoints.
func NewFileTransactionLogger(filename string, grpcEndpoint string) (*FileTransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log File '%s': %w", filename, err)
	}

	auditFilename := filename + ".audit"
	auditFile, err := os.OpenFile(auditFilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		_ = file.Close()
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

	return &FileTransactionLogger{
		FileTransactionLogger: &schemas.FileTransactionLogger{File: file},
		eventsAudit:           make(chan *accesslog.StreamAccessLogsMessage, 16),
		errorsAudit:           make(chan error, 1),
		auditFile:             auditFile,
		grpcClient:            grpcClient,
		grpcConn:              grpcConn,
		stop:                  make(chan struct{}),
		stopped:               make(chan struct{}),
	}, nil
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

// RunAudit starts the loop that delivers audit events until Shutdown.
func (l *FileTransactionLogger) RunAudit() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for {
			select {
			case e := <-l.eventsAudit:
				l.processAudit(e)
			case err := <-l.errorsAudit:
				log.Error().Err(err).Msg("audit processing error")
			case <-l.stop:
				for {
					select {
					case e := <-l.eventsAudit:
						l.processAudit(e)
					default:
						return
					}
				}
			}
		}
	}()
}

func (l *FileTransactionLogger) processAudit(e *accesslog.StreamAccessLogsMessage) {
	l.auditToFile(e)
	l.auditToGRPC(e)
}

// Shutdown stops the processing loops after they have written every queued
// event, then closes files and the gRPC connection. It is safe to call more
// than once. Events submitted afterwards are dropped instead of blocking.
func (l *FileTransactionLogger) Shutdown() {
	l.shutdownOnce.Do(func() {
		close(l.stop)
		l.wg.Wait()
		close(l.stopped)
		l.CloseAll()
	})
}

// CloseAudit queues an audit event; on a full queue it waits up to a second, then drops it.
func (l *FileTransactionLogger) CloseAudit(e *accesslog.StreamAccessLogsMessage) {
	select {
	case l.eventsAudit <- e:
		return
	default:
	}

	select {
	case l.eventsAudit <- e:
	case <-l.stopped:
	case <-time.After(time.Second):
		log.Error().Msg("audit channel is full, dropping audit entry")
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

	fileMetric := zerolog.New(l.File)
	fileMetric.Log().
		Str(consts.FieldMetricRquid, requestId).
		Str("metric_name", metricName).
		Str("metric_value", metricValue).
		Send()
}

// auditToFile writes audit events to the audit file as JSON lines
func (l *FileTransactionLogger) auditToFile(msg *accesslog.StreamAccessLogsMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		l.reportAuditError(fmt.Errorf("failed to marshal StreamAccessLogsMessage: %w", err))
		return
	}

	if _, err := l.auditFile.Write(append(data, '\n')); err != nil {
		l.reportAuditError(fmt.Errorf("failed to write audit event to file: %w", err))
		return
	}

	if err := l.auditFile.Sync(); err != nil {
		l.reportAuditError(fmt.Errorf("failed to sync audit file: %w", err))
	}
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

// auditToGRPC sends audit events via gRPC
func (l *FileTransactionLogger) auditToGRPC(msg *accesslog.StreamAccessLogsMessage) {
	if l.grpcClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := l.grpcClient.StreamAccessLogs(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to create stream for access logs")
		return
	}

	if err := stream.Send(msg); err != nil {
		log.Error().Err(err).Msg("access log failed to send access log message")
		return
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

// CloseAll closes every resource, reporting (but not stopping at) individual failures.
func (l *FileTransactionLogger) CloseAll() {
	if err := l.File.Close(); err != nil {
		log.Error().Msgf("error closing file '%s': %v", l.File.Name(), err)
	}
	if err := l.auditFile.Close(); err != nil {
		log.Error().Msgf("error closing file '%s': %v", l.auditFile.Name(), err)
	}
	if l.grpcConn != nil {
		if err := l.grpcConn.Close(); err != nil {
			log.Error().Msgf("error closing grpc conn: %v", err)
		}
	}
}
