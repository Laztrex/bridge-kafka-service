package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
)

// Secret masking shared by the Go services (platform spec core/platform-logging): registered
// values are replaced with secretMask in everything written through Redacting, including their
// JSON-escaped form. Keep this file identical across repositories.

const secretMask = "***"

// minSecretLen: shorter values are not masked, otherwise "a" would garble the log.
const minSecretLen = 4

var redactor struct {
	mu      sync.RWMutex
	secrets [][]byte
}

// RegisterSecrets registers values that must never appear in log output.
func RegisterSecrets(values ...string) {
	redactor.mu.Lock()
	defer redactor.mu.Unlock()
	for _, v := range values {
		if len(v) < minSecretLen {
			continue
		}
		redactor.secrets = append(redactor.secrets, []byte(v))
		if b, err := json.Marshal(v); err == nil {
			if escaped := b[1 : len(b)-1]; string(escaped) != v {
				redactor.secrets = append(redactor.secrets, escaped)
			}
		}
	}
}

// Redact replaces every registered secret in s with the mask.
func Redact(s string) string {
	return string(redact([]byte(s)))
}

func redact(p []byte) []byte {
	redactor.mu.RLock()
	defer redactor.mu.RUnlock()
	for _, secret := range redactor.secrets {
		if bytes.Contains(p, secret) {
			p = bytes.ReplaceAll(p, secret, []byte(secretMask))
		}
	}
	return p
}

// Redacting wraps w so that every write is masked. It reports len(p) as written, so a shorter
// masked record does not look like a short write to the logger.
func Redacting(w io.Writer) io.Writer { return redactingWriter{w} }

type redactingWriter struct{ w io.Writer }

func (r redactingWriter) Write(p []byte) (int, error) {
	if _, err := r.w.Write(redact(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// resetSecrets forgets every registered secret; for tests.
func resetSecrets() {
	redactor.mu.Lock()
	defer redactor.mu.Unlock()
	redactor.secrets = nil
}
