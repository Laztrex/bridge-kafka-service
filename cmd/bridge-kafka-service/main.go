package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/app"
	"bridge-kafka-service/internal/codec"
	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/logging"
)

const appName = "bridge-kafka-service"

// Build metadata, set via -ldflags "-X main.version=..." (see Makefile and Dockerfile).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
	branch  = "dev"
	builtBy = "unknown"
)

// options are the command-line flags. Both the long and the short forms of the former
// cobra CLI are accepted: -config/--config/-c and -version/--version/-v.
type options struct {
	configPath  string
	showVersion bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// parseFlags returns the options, or exit code >= 0 when the process must exit right away
// (0 for -help, 1 for a usage error, as cobra did).
func parseFlags(args []string, stderr io.Writer) (options, int) {
	var opts options
	fs := flag.NewFlagSet(appName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opts.configPath, "config", "", "config file path (default: $CONFIG_PATH or configs/config_app.yaml)")
	fs.StringVar(&opts.configPath, "c", "", "shorthand for -config")
	fs.BoolVar(&opts.showVersion, "version", false, "print the version and exit")
	fs.BoolVar(&opts.showVersion, "v", false, "shorthand for -version")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return opts, 0
		}
		return opts, 1
	}
	return opts, -1
}

func versionLine() string {
	return fmt.Sprintf("%s version %s (commit %s, built %s, %s)", appName, version, commit, date, runtime.Version())
}

func run(args []string, stdout, stderr io.Writer) int {
	opts, code := parseFlags(args, stderr)
	if code >= 0 {
		return code
	}
	if opts.showVersion {
		_, _ = fmt.Fprintln(stdout, versionLine())
		return 0
	}

	logging.Init(config.LogFilePath(os.Getenv))
	path, usedDefault := config.ResolvePath(opts.configPath, os.Getenv)
	if usedDefault {
		log.Warn().Msgf("Config file is not specified. Using default: %s", path)
	}
	cfg, err := config.Load(path, os.Getenv)
	if err != nil {
		log.Error().Err(err).Msg("unable to load the config")
		return 1
	}
	logging.SetLevel(cfg.LogLevel)
	for _, key := range cfg.Warnings {
		log.Warn().Msgf("unknown config key %q is ignored", key)
	}
	codec.SetSystemID(cfg.SystemID)

	log.Info().
		Str("version", version).Str("commit", commit).Str("date", date).
		Str("branch", branch).Str("built_by", builtBy).
		Str("go_version", runtime.Version()).Str("os", runtime.GOOS).Str("arch", runtime.GOARCH).
		Msg("starting")
	config.LogKafkaConfig(cfg.Kafka)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.Connect(ctx, cfg); err != nil {
		log.Error().Err(err).Msg("service stopped with error")
		return 1
	}
	return 0
}
