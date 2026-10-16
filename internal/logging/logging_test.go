package logging

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func restoreGlobals(t *testing.T) {
	t.Helper()
	oldLogger, oldLevel, oldSarama := log.Logger, zerolog.GlobalLevel(), sarama.Logger
	t.Cleanup(func() {
		log.Logger, sarama.Logger = oldLogger, oldSarama
		zerolog.SetGlobalLevel(oldLevel)
		resetSecrets()
	})
}

func TestSpecificLevelWriterFiltersByLevel(t *testing.T) {
	var buf bytes.Buffer
	w := SpecificLevelWriter{Writer: &buf, Levels: []zerolog.Level{zerolog.InfoLevel}}
	if n, err := w.WriteLevel(zerolog.InfoLevel, []byte("kept")); n != 4 || err != nil {
		t.Fatalf("kept: %d %v", n, err)
	}
	if n, err := w.WriteLevel(zerolog.DebugLevel, []byte("dropped")); n != 7 || err != nil {
		t.Fatalf("filtered records are reported as written: %d %v", n, err)
	}
	if buf.String() != "kept" {
		t.Fatalf("buffer: %q", buf.String())
	}
}

// The run log file gets JSON with the platform fields and only info and error records.
func TestRunLogFileFormatAndLevels(t *testing.T) {
	for _, level := range []string{"debug", "info"} {
		t.Run(level, func(t *testing.T) {
			restoreGlobals(t)
			file := filepath.Join(t.TempDir(), "app.log")
			Init(file)
			SetLevel(level)
			want := zerolog.InfoLevel
			if level == "debug" {
				want = zerolog.DebugLevel
			}
			if zerolog.GlobalLevel() != want {
				t.Fatalf("level: %v", zerolog.GlobalLevel())
			}
			log.Debug().Msg("debug record")
			log.Warn().Msg("warn record")
			log.Error().Msg("error record")

			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			var msgs []string
			for _, line := range lines {
				var rec map[string]any
				if err := json.Unmarshal([]byte(line), &rec); err != nil {
					t.Fatalf("not JSON: %q", line)
				}
				for _, f := range []string{"rqTime", "loglevel", "message", "funcName", "host", "service"} {
					if _, ok := rec[f]; !ok {
						t.Fatalf("field %s missing: %s", f, line)
					}
				}
				msgs = append(msgs, rec["message"].(string))
			}
			if got := strings.Join(msgs, "|"); got != "LEVEL "+level+"|error record" {
				t.Fatalf("records in the file: %q", got)
			}
		})
	}
}

func TestRedactingMasksSecrets(t *testing.T) {
	restoreGlobals(t)
	RegisterSecrets("s3cr3t-token", `quote"d\secret`, "ab")

	var buf bytes.Buffer
	w := Redacting(&buf)
	line := `{"message":"token s3cr3t-token","err":"quote\"d\\secret","short":"ab"}`
	if n, err := w.Write([]byte(line)); n != len(line) || err != nil {
		t.Fatalf("write: %d %v", n, err)
	}
	out := buf.String()
	if strings.Contains(out, "s3cr3t-token") || strings.Contains(out, `quote\"d\\secret`) {
		t.Fatalf("secret leaked: %s", out)
	}
	if !strings.Contains(out, "***") || !strings.Contains(out, `"short":"ab"`) {
		t.Fatalf("mask missing or a short value masked: %s", out)
	}
	if Redact("x s3cr3t-token y") != "x *** y" {
		t.Fatal("Redact")
	}
}

func TestSecretsNeverReachTheRunLog(t *testing.T) {
	restoreGlobals(t)
	file := filepath.Join(t.TempDir(), "app.log")
	Init(file)
	SetLevel("info")
	RegisterSecrets("kafka-password-1")
	log.Error().Str("dsn", "user:kafka-password-1@host").Msg("connect kafka-password-1 failed")

	raw, _ := os.ReadFile(file)
	if strings.Contains(string(raw), "kafka-password-1") || !strings.Contains(string(raw), "***") {
		t.Fatalf("run log: %s", raw)
	}
}
