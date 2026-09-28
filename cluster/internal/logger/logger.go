package logger

import (
	"log/slog"
	"sync/atomic"
)

var logger atomic.Pointer[slog.Logger]

func init() {
	logger.Store(slog.Default())
}

// SetLogger sets the logger.
func SetLogger(l *slog.Logger) {
	logger.Store(l)
}

// Log returns the logger.
func Log() *slog.Logger {
	return logger.Load()
}
