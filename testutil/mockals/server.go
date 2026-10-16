// Package mockals runs a loopback Envoy AccessLogService gRPC server so the
// audit delivery path can be exercised with real gRPC code and no external service.
package mockals

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"
	"google.golang.org/grpc"
)

// Server records every StreamAccessLogsMessage it receives.
type Server struct {
	accesslog.UnimplementedAccessLogServiceServer

	// Addr is the host:port the server listens on.
	Addr string

	mu       sync.Mutex
	messages []*accesslog.StreamAccessLogsMessage
	failWith error
	grpc     *grpc.Server
}

// Start listens on 127.0.0.1:0 and stops the server when the test ends.
func Start(t testing.TB) *Server {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("mockals: listen: %v", err)
	}
	s := &Server{Addr: lis.Addr().String(), grpc: grpc.NewServer()}
	accesslog.RegisterAccessLogServiceServer(s.grpc, s)
	go func() { _ = s.grpc.Serve(lis) }()
	t.Cleanup(s.grpc.Stop)
	return s
}

// FailWith makes every following stream end with err after its first message.
func (s *Server) FailWith(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failWith = err
}

// Messages returns a snapshot of the received messages.
func (s *Server) Messages() []*accesslog.StreamAccessLogsMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*accesslog.StreamAccessLogsMessage(nil), s.messages...)
}

// StreamAccessLogs implements accesslog.AccessLogServiceServer.
func (s *Server) StreamAccessLogs(stream accesslog.AccessLogService_StreamAccessLogsServer) error {
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&accesslog.StreamAccessLogsResponse{})
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.messages = append(s.messages, msg)
		fail := s.failWith
		s.mu.Unlock()
		if fail != nil {
			return fail
		}
	}
}
