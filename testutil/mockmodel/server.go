// Package mockmodel runs a loopback BaseModelInterface gRPC server so the gRPC
// upstream can be exercised with real gRPC code (optionally over TLS/mTLS).
package mockmodel

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"bridge-kafka-service/internal/transport/modelrpc"
)

// Received is one Predict request as seen by the server.
type Received struct {
	Details  map[string]string
	Data     []byte
	Metadata metadata.MD
}

// Server records requests and answers with whatever the test programmed.
type Server struct {
	modelrpc.UnimplementedBaseModelInterfaceServer

	// Addr is the host:port the server listens on.
	Addr string

	mu       sync.Mutex
	received []Received
	response []byte
	header   metadata.MD
	trailer  metadata.MD
	err      error
	delay    time.Duration
	grpc     *grpc.Server
}

// Option customises Start.
type Option func(*config)

type config struct {
	tls  *tls.Config
	addr string
}

// WithTLS serves over TLS using cfg (build it with testcerts.Bundle.ServerConfig).
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.tls = cfg } }

// WithAddr listens on a specific address, e.g. to bring a stopped server back.
func WithAddr(addr string) Option { return func(c *config) { c.addr = addr } }

// Start listens (127.0.0.1:0 by default) and stops with the test.
func Start(t testing.TB, opts ...Option) *Server {
	t.Helper()
	cfg := config{addr: "127.0.0.1:0"}
	for _, o := range opts {
		o(&cfg)
	}
	lis, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		t.Fatalf("mockmodel: listen: %v", err)
	}

	serverOpts := []grpc.ServerOption{}
	if cfg.tls != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(cfg.tls)))
	}
	s := &Server{Addr: lis.Addr().String(), response: []byte(`{}`), grpc: grpc.NewServer(serverOpts...)}
	modelrpc.RegisterBaseModelInterfaceServer(s.grpc, s)
	go func() { _ = s.grpc.Serve(lis) }()
	t.Cleanup(s.Stop)
	return s
}

// Stop shuts the server down immediately (safe to call twice).
func (s *Server) Stop() { s.grpc.Stop() }

// Respond sets the data returned by successful calls.
func (s *Server) Respond(data []byte) { s.set(func() { s.response = data }) }

// RespondMetadata sets header and trailer metadata sent with successful calls.
func (s *Server) RespondMetadata(header, trailer metadata.MD) {
	s.set(func() { s.header, s.trailer = header, trailer })
}

// Fail makes every call return err (use status.Error for a gRPC status).
func (s *Server) Fail(err error) { s.set(func() { s.err = err }) }

// Delay makes every call wait before answering (or until the caller gives up).
func (s *Server) Delay(d time.Duration) { s.set(func() { s.delay = d }) }

// Requests returns a snapshot of the received requests.
func (s *Server) Requests() []Received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Received(nil), s.received...)
}

func (s *Server) set(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
}

// Predict implements modelrpc.BaseModelInterfaceServer.
func (s *Server) Predict(ctx context.Context, req *modelrpc.ModelInterfaceRequest) (*modelrpc.ModelInterfaceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.received = append(s.received, Received{Details: req.GetDetails(), Data: req.GetData(), Metadata: md})
	resp, header, trailer, err, delay := s.response, s.header, s.trailer, s.err, s.delay
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if header != nil {
		_ = grpc.SetHeader(ctx, header)
	}
	if trailer != nil {
		_ = grpc.SetTrailer(ctx, trailer)
	}
	if err != nil {
		return nil, err
	}
	return &modelrpc.ModelInterfaceResponse{Data: resp}, nil
}
