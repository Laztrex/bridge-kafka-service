// Command example-grpc-model is a minimal gRPC model service for trying the
// bridge's `service.protocol: grpc` upstream. It implements
// controller_kafka.BaseModelInterface/Predict and echoes what it receives:
//
//	request  data = {"params":[1,2,3]}   details = {"x-request-id":"abc", ...}
//	response data = {"echo":{"params":[1,2,3]},"request_id":"abc","service":"example-grpc-model"}
//
// It can serve plaintext (default), TLS (-tls-cert/-tls-key) or mutual TLS (plus
// -client-ca). It is a demo, not a production service.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"bridge-kafka-service/internal/transport/modelrpc"
)

const serviceName = "example-grpc-model"

type server struct {
	modelrpc.UnimplementedBaseModelInterfaceServer
}

// Predict echoes the payload together with the request id found in details. Its response
// metadata reaches the bridge as response headers.
func (server) Predict(ctx context.Context, req *modelrpc.ModelInterfaceRequest) (*modelrpc.ModelInterfaceResponse, error) {
	requestID := req.GetDetails()["x-request-id"]
	log.Printf("Predict request_id=%q details=%v data=%s", requestID, req.GetDetails(), req.GetData())

	var echo any = string(req.GetData())
	if json.Valid(req.GetData()) {
		echo = json.RawMessage(req.GetData())
	}
	out, err := json.Marshal(map[string]any{"echo": echo, "request_id": requestID, "service": serviceName})
	if err != nil {
		return nil, err
	}

	_ = grpc.SetHeader(ctx, metadata.Pairs("x-client-id", serviceName))
	return &modelrpc.ModelInterfaceResponse{Data: out}, nil
}

// serverOptions builds transport credentials from the TLS flags: none, TLS, or mTLS.
func serverOptions(certFile, keyFile, clientCA string) ([]grpc.ServerOption, error) {
	if certFile == "" && keyFile == "" {
		if clientCA != "" {
			return nil, fmt.Errorf("-client-ca requires -tls-cert and -tls-key")
		}
		return nil, nil
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	if clientCA != "" {
		pem, err := os.ReadFile(clientCA)
		if err != nil {
			return nil, fmt.Errorf("read -client-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("-client-ca %q has no valid PEM certificate", clientCA)
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(cfg))}, nil
}

func main() {
	addr := flag.String("addr", ":50051", "listen address")
	certFile := flag.String("tls-cert", "", "server certificate (enables TLS)")
	keyFile := flag.String("tls-key", "", "server private key")
	clientCA := flag.String("client-ca", "", "CA that must have signed client certificates (enables mutual TLS)")
	flag.Parse()

	opts, err := serverOptions(*certFile, *keyFile, *clientCA)
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	s := grpc.NewServer(opts...)
	modelrpc.RegisterBaseModelInterfaceServer(s, server{})

	mode := "plaintext"
	switch {
	case *clientCA != "":
		mode = "mutual TLS"
	case *certFile != "":
		mode = "TLS"
	}
	log.Printf("%s listening on %s (%s)", serviceName, lis.Addr(), mode)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-stop; s.GracefulStop() }()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
