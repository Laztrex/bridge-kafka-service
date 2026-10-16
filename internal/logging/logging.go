// Package logging configures the platform log format (docs/conventions.md, section 6): zerolog
// records with rqTime, loglevel, message, funcName, host and service; stdout gets a plain text
// console format, the run log file gets JSON with info and error records only.
package logging

import (
	"fmt"
	"io"
	logger "log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Service is the value of the service field.
const Service = "bridge-kafka-service"

// SpecificLevelWriter passes only records of the listed levels to Writer.
type SpecificLevelWriter struct {
	io.Writer
	Levels []zerolog.Level
}

// WriteLevel writes p if level is listed; filtered records are reported as written.
func (w SpecificLevelWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	for _, l := range w.Levels {
		if l == level {
			return w.Write(p)
		}
	}
	return len(p), nil
}

// Init installs the global logger: stdout (debug to error, text) and logFile (info and error,
// JSON, rotated), both through the secret redactor. The level is set separately by SetLevel once
// the config is read.
func Init(logFile string) {
	zerolog.LevelFieldName = "loglevel"
	zerolog.TimestampFieldName = "rqTime"
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.CallerFieldName = "funcName"

	runLogFile := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    5,
		MaxAge:     7,
		MaxBackups: 2,
		LocalTime:  false,
		Compress:   true,
	}

	multi := zerolog.MultiLevelWriter(
		SpecificLevelWriter{
			Writer: zerolog.ConsoleWriter{
				Out:           Redacting(os.Stdout),
				NoColor:       true,
				TimeFormat:    time.RFC3339,
				PartsOrder:    []string{"rqTime", "loglevel", "message"},
				FieldsExclude: []string{"host", "service", "funcName"},
				FormatMessage: func(i interface{}) string { return fmt.Sprintf("|%5v", i) },
				FormatLevel:   func(i interface{}) string { return strings.ToUpper(fmt.Sprintf("|%5s", i)) },
			},
			Levels: []zerolog.Level{zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel}},
		SpecificLevelWriter{
			Writer: Redacting(runLogFile),
			Levels: []zerolog.Level{zerolog.InfoLevel, zerolog.ErrorLevel}},
	)

	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	log.Logger = zerolog.New(multi).With().Timestamp().Caller().Str("host", host).Str("service", Service).Logger()
}

// SetLevel applies logLevel from the config: "debug" enables debug records and the Kafka client
// log; anything else means info.
func SetLevel(level string) {
	if level == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		sarama.Logger = logger.New(os.Stderr, "[Sarama] ", logger.LstdFlags)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Info().Msgf("LEVEL %v", level)
}
