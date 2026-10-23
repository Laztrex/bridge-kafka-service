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
	"strings"
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
	checkConfig bool
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
	fs.BoolVar(&opts.checkConfig, "check-config", false, "check the configuration and exit: 0 if it is valid, 1 otherwise")
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
	if opts.checkConfig {
		return checkConfig(opts.configPath, stdout, stderr)
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
	// The deprecation warnings are logged before the configured level applies, so they are
	// visible whatever log.level says.
	for _, d := range cfg.Deprecations {
		log.Warn().Msg(d)
	}
	logging.SetLevel(cfg.LogLevel)
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

// checkConfig loads the configuration and runs exactly the checks startup runs, without setting up
// logging, connecting anywhere or writing files. Warnings and errors go to stderr, one per line,
// and the verdict to stdout; it returns 0 when the configuration is valid and 1 otherwise.
// A file that parsed but is invalid still has its remaining checks run, so every problem is
// reported in one pass.
func checkConfig(flagPath string, stdout, stderr io.Writer) int {
	path, usedDefault := config.ResolvePath(flagPath, os.Getenv)
	if usedDefault {
		_, _ = fmt.Fprintf(stderr, "warning: config file is not specified, using default %s\n", path)
	}
	cfg, err := config.Load(path, os.Getenv)
	if err != nil {
		for _, line := range strings.Split(err.Error(), "\n") {
			_, _ = fmt.Fprintf(stderr, "error: %s\n", line)
		}
	}
	if cfg != nil {
		for _, d := range cfg.Deprecations {
			_, _ = fmt.Fprintf(stderr, "warning: %s\n", d)
		}
		warnings, checkErr := app.Check(cfg)
		for _, w := range warnings {
			_, _ = fmt.Fprintf(stderr, "warning: %s\n", w)
		}
		if checkErr != nil {
			err = errors.Join(err, checkErr)
			for _, line := range strings.Split(checkErr.Error(), "\n") {
				_, _ = fmt.Fprintf(stderr, "error: %s\n", line)
			}
		}
	}
	if err != nil {
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "configuration %s is valid\n", path)
	return 0
}
