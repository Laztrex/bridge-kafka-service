package mockmodel

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"bridge-kafka-service/internal/transport/modelrpc"
)

func TestServerRecordsAndAnswers(t *testing.T) {
	srv := Start(t)
	srv.Respond([]byte(`{"ok":1}`))
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := modelrpc.NewBaseModelInterfaceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Predict(ctx, &modelrpc.ModelInterfaceRequest{Details: map[string]string{"k": "v"}, Data: []byte("x")})
	if err != nil || string(resp.GetData()) != `{"ok":1}` {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	if got := srv.Requests(); len(got) != 1 || got[0].Details["k"] != "v" || string(got[0].Data) != "x" {
		t.Fatalf("requests=%+v", got)
	}

	srv.Fail(status.Error(codes.InvalidArgument, "bad"))
	if _, err := client.Predict(ctx, &modelrpc.ModelInterfaceRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err=%v", err)
	}
}
