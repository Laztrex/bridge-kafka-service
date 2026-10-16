package transport

import (
	"context"
	"fmt"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport/modelrpc"
)

// grpcMaxMessageBytes is the send/receive limit of the client (gRPC's default
// is 4 MiB, smaller than payloads Kafka commonly carries).
const grpcMaxMessageBytes = 16 << 20

// Headers published for a call that ended with an application status.
const (
	headerGrpcStatus  = "Grpc-Status"
	headerGrpcMessage = "Grpc-Message"
)

type grpcUpstream struct {
	conn   *grpc.ClientConn
	client modelrpc.BaseModelInterfaceClient
}

// NewGRPCUpstream returns the gRPC implementation of Upstream: one unary
// BaseModelInterface.Predict call per record over a single long-lived
// connection. The connection is established lazily, so an unreachable service
// never blocks startup. Close it when the pipeline stops.
func NewGRPCUpstream(conf *schemas.ConsumerConfig) (Upstream, error) {
	creds := credentials.TransportCredentials(insecure.NewCredentials())
	if conf.UpstreamTLS != nil {
		creds = credentials.NewTLS(conf.UpstreamTLS.Clone())
	}
	conn, err := grpc.NewClient(conf.Service.Target(),
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcMaxMessageBytes),
			grpc.MaxCallSendMsgSize(grpcMaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc client for %s: %w", conf.Service.Target(), err)
	}
	return &grpcUpstream{conn: conn, client: modelrpc.NewBaseModelInterfaceClient(conn)}, nil
}

// Close releases the connection.
func (u *grpcUpstream) Close() error { return u.conn.Close() }

// Call performs one Predict call. A gRPC status from the service is an answer (published with
// Grpc-Status and no data); only an unreachable service or a timeout is a transport failure.
func (u *grpcUpstream) Call(ctx context.Context, req UpstreamRequest) (UpstreamResponse, error) {
	if id := req.Headers[consts.HeaderRequestIdKey]; id != "" && printableASCII(id) {
		ctx = metadata.AppendToOutgoingContext(ctx, consts.HeaderRequestIdKey, id)
	}

	var header, trailer metadata.MD
	resp, err := u.client.Predict(ctx,
		&modelrpc.ModelInterfaceRequest{Details: req.Headers, Data: req.Body},
		grpc.Header(&header), grpc.Trailer(&trailer))

	out := UpstreamResponse{Header: http.Header{}}
	for _, md := range []metadata.MD{header, trailer} {
		for k, values := range md {
			for _, v := range values {
				out.Header.Add(k, v)
			}
		}
	}

	if err == nil {
		out.Body = resp.GetData()
		return out, nil
	}

	st := status.Convert(err)
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return UpstreamResponse{}, err
	default:
		out.Header.Set(headerGrpcStatus, st.Code().String())
		out.Header.Set(headerGrpcMessage, st.Message())
		return out, nil
	}
}

// printableASCII reports whether s is valid as a gRPC metadata value.
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}
