package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The flags of the former cobra CLI keep working: long and short forms, -- and - prefixes.
func TestParseFlagsKeepsFormerCLI(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		config  string
		version bool
		code    int
	}{
		{"no flags", nil, "", false, -1},
		{"--config", []string{"--config", "a.yaml"}, "a.yaml", false, -1},
		{"--config=", []string{"--config=a.yaml"}, "a.yaml", false, -1},
		{"-config", []string{"-config", "a.yaml"}, "a.yaml", false, -1},
		{"-c", []string{"-c", "a.yaml"}, "a.yaml", false, -1},
		{"--version", []string{"--version"}, "", true, -1},
		{"-v", []string{"-v"}, "", true, -1},
		{"--help exits 0", []string{"--help"}, "", false, 0},
		{"unknown flag exits 1", []string{"--nope"}, "", false, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, code := parseFlags(tc.args, &bytes.Buffer{})
			if code != tc.code || opts.configPath != tc.config || opts.showVersion != tc.version {
				t.Fatalf("got config=%q version=%v code=%d, want config=%q version=%v code=%d",
					opts.configPath, opts.showVersion, code, tc.config, tc.version, tc.code)
			}
		})
	}
}

func TestRunPrintsVersion(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"--version"}, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if !strings.HasPrefix(out.String(), "bridge-kafka-service version ") {
		t.Fatalf("unexpected version line: %q", out.String())
	}
}

// checkDir prepares a working directory holding the consumer tuning file and an app config with
// the given body, and returns the config path.
func checkDir(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll(filepath.Join(dir, "configs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "configs", "config_consumer.yaml"), []byte("consumer:\n  return:\n    errors: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "app.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validConfig = `
kafka:
  brokers: 127.0.0.1:1
  version: 2.3.0
consumer:
  - topics: orders
    group: g
    service:
      name: svc
      port: "8080"
`

func TestCheckConfig_ValidConfiguration(t *testing.T) {
	path := checkDir(t, validConfig)
	var out, errOut bytes.Buffer

	code := run([]string{"--check-config", "--config", path}, &out, &errOut)

	if code != 0 {
		t.Fatalf("exit code %d, stderr:\n%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "is valid") {
		t.Fatalf("stdout must state the configuration is valid: %q", out.String())
	}
	if errOut.Len() != 0 {
		t.Fatalf("a valid configuration prints no warnings, stderr: %q", errOut.String())
	}
}

// An unknown key fails --check-config with exit code 1 and the key named on stderr.
func TestCheckConfig_UnknownKey(t *testing.T) {
	path := checkDir(t, validConfig+"unknown_key: 1\n")
	var out, errOut bytes.Buffer

	code := run([]string{"--check-config", "--config", path}, &out, &errOut)

	if code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), `error: unknown config key "unknown_key"`) {
		t.Fatalf("the unknown key must be named: %q", errOut.String())
	}
	if strings.Contains(out.String(), "is valid") {
		t.Fatalf("stdout must not state the configuration is valid: %q", out.String())
	}
}

// A deprecated alias still loads and is reported as a warning.
func TestCheckConfig_DeprecatedAlias(t *testing.T) {
	path := checkDir(t, validConfig+"http.addr: 0.0.0.0:9000\n")
	var out, errOut bytes.Buffer

	code := run([]string{"--check-config", "--config", path}, &out, &errOut)

	if code != 0 {
		t.Fatalf("exit code %d, stderr:\n%s", code, errOut.String())
	}
	if !strings.Contains(errOut.String(), "warning: http.addr is deprecated, use health.addr") {
		t.Fatalf("the deprecation must be printed: %q", errOut.String())
	}
}

func TestCheckConfig_InvalidConfiguration(t *testing.T) {
	path := checkDir(t, strings.Replace(validConfig, "group: g", "group: g\n    response_topic: orders", 1))
	var out, errOut bytes.Buffer

	code := run([]string{"--check-config", "--config", path}, &out, &errOut)

	if code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), "error: ") || !strings.Contains(errOut.String(), "orders -> orders") {
		t.Fatalf("the cycle must be printed as an error: %q", errOut.String())
	}
	if out.Len() != 0 {
		t.Fatalf("nothing is printed to stdout for an invalid configuration: %q", out.String())
	}
}

func TestCheckConfig_UnreadableFile(t *testing.T) {
	checkDir(t, validConfig)
	var errOut bytes.Buffer

	code := run([]string{"--check-config", "--config", "missing.yaml"}, &bytes.Buffer{}, &errOut)

	if code != 1 || !strings.Contains(errOut.String(), "error: read config") {
		t.Fatalf("exit %d, stderr %q", code, errOut.String())
	}
}

func TestCheckConfig_WritesNoFiles(t *testing.T) {
	path := checkDir(t, validConfig)
	t.Setenv("LOG_PATH", filepath.Dir(path))
	before, _ := os.ReadDir(filepath.Dir(path))

	run([]string{"--check-config", "--config", path}, &bytes.Buffer{}, &bytes.Buffer{})

	after, _ := os.ReadDir(filepath.Dir(path))
	if len(after) != len(before) {
		t.Fatalf("check mode created files: before %v, after %v", before, after)
	}
}
