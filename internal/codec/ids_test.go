package codec

import "testing"

func TestNewRequestID(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if len(a) != 36 || a == b {
		t.Fatalf("ids: %q %q", a, b)
	}
}

func TestSystemID(t *testing.T) {
	SetSystemID("")
	if SystemID() != "" {
		t.Fatal("an unset SYSTEM_ID yields an empty value, not a failure")
	}
	SetSystemID("sys-1")
	t.Cleanup(func() { SetSystemID("") })
	if SystemID() != "sys-1" {
		t.Fatal("SystemID")
	}
}
