package modelrpc

import "testing"

// The gRPC method name is a legacy wire identifier agreed with the model
// services (spec project-identity): regenerating or moving the stubs must not
// change it.
func TestWireIdentityIsFrozen(t *testing.T) {
	if got, want := BaseModelInterface_Predict_FullMethodName, "/controller_kafka.BaseModelInterface/Predict"; got != want {
		t.Fatalf("method name = %q, want %q", got, want)
	}
	if got, want := BaseModelInterface_ServiceDesc.ServiceName, "controller_kafka.BaseModelInterface"; got != want {
		t.Fatalf("service name = %q, want %q", got, want)
	}
}
