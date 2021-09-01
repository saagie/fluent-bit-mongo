package log

import (
	"context"
	"errors"
)

var loggerContextKey = "logger"

var ErrNoLoggerFound = errors.New("no logger found")

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, &loggerContextKey, logger)
}

func GetLogger(ctx context.Context) (Logger, error) {
	logger := ctx.Value(&loggerContextKey)
	if logger == nil {
		return nil, ErrNoLoggerFound
	}

	return logger.(Logger), nil
}
