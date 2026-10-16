package schemas

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostConfigDefaults(t *testing.T) {
	h := &HostConfig{Name: "svc", Port: "8080", Path: "/p"}

	assert.Equal(t, ProtocolHTTP, h.EffectiveProtocol(), "protocol defaults to http")
	assert.Equal(t, TLSModeNone, h.TLS.EffectiveMode(), "encryption defaults to none")
	assert.False(t, h.TLS.Encrypted())
	assert.Equal(t, "http://svc:8080/p", h.URI())
	assert.Equal(t, "svc:8080", h.Target())
}

func TestHostConfigSchemeFollowsTLSMode(t *testing.T) {
	for mode, want := range map[string]string{
		"": "http://svc:1/p", "none": "http://svc:1/p", "NONE": "http://svc:1/p",
		"tls": "https://svc:1/p", "mtls": "https://svc:1/p", " TLS ": "https://svc:1/p",
	} {
		h := &HostConfig{Name: "svc", Port: "1", Path: "/p", TLS: ServiceTLS{Mode: mode}}
		assert.Equal(t, want, h.URI(), "mode %q", mode)
	}
}

func TestEffectiveProtocolIsCaseInsensitive(t *testing.T) {
	assert.Equal(t, ProtocolGRPC, (&HostConfig{Protocol: " GRPC "}).EffectiveProtocol())
}

func TestEffectiveFailurePolicy(t *testing.T) {
	for in, want := range map[string]string{
		"": FailureSkip, "skip": FailureSkip, "retry": FailureRetry, " RETRY ": FailureRetry, "park": "park",
	} {
		assert.Equal(t, want, (&ConsumerConfig{OnFailure: in}).EffectiveFailurePolicy(), "on_failure %q", in)
	}
}
