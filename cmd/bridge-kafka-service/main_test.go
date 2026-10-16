package main

import (
	"bytes"
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
