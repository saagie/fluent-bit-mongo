package log

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/saagie/fluent-bit-mongo/pkg/log/encoder"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// https://github.com/fluent/fluent-bit/blob/03f3339594bdb472315823db78ca209b7ba319fb/src/flb_config.c#L433-L441
const FluentBitLogLevelEnvironment = "FLB_LOG_LEVEL"

type Logger interface {
	Debug(string, map[string]interface{})
	Info(string, map[string]interface{})
	Error(string, map[string]interface{})
}

type logger struct {
	log logr.Logger
}

var registerEncoderOnce sync.Once

func New(t PluginType, name string) (Logger, error) {
	if len(name) == 0 {
		return nil, errors.New("empty name")
	}

	zc := zap.NewProductionConfig()

	level := zapcore.InfoLevel
	levelStr := os.Getenv(FluentBitLogLevelEnvironment)
	if levelStr == "" {
		if err := level.UnmarshalText([]byte(levelStr)); err != nil {
			return nil, fmt.Errorf("invalid level %s: %w", levelStr, err)
		}
	}
	zc.Level = zap.NewAtomicLevelAt(level)
	zc.DisableStacktrace = true

	var err error
	registerEncoderOnce.Do(func() {
		err = zap.RegisterEncoder("fluent-bit", encoder.New)
	})
	if err != nil {
		return nil, fmt.Errorf("cannot register fluent-bit encoder: %w", err)
	}

	zc.Encoding = "fluent-bit"

	z, err := zc.Build()
	if err != nil {
		return nil, fmt.Errorf("build log config: %w", err)
	}

	log := zapr.NewLogger(z)

	return &logger{
		log: log.WithName(fmt.Sprintf("%s:%s", t.String(), name)),
	}, nil
}

func (l *logger) Debug(message string, args map[string]interface{}) {
	if logger := l.log.V(1); logger.Enabled() {
		logger.Info(message, l.argsToMeta(args)...)
	}
}

func (l *logger) Info(message string, args map[string]interface{}) {
	if logger := l.log; logger.Enabled() {
		logger.Info(message, l.argsToMeta(args)...)
	}
}

func (l *logger) Error(message string, args map[string]interface{}) {
	var err error
	if err2, ok := args["error"]; ok {
		err = err2.(error)
		delete(args, "error")
	}

	l.log.Error(err, message, l.argsToMeta(args)...)
}

func (l *logger) argsToMeta(args map[string]interface{}) []interface{} {
	meta := make([]interface{}, len(args)*2)

	i := 0
	for k, v := range args {
		meta[i] = k
		i += 1
		meta[i] = v
		i += 1
	}

	return meta
}
