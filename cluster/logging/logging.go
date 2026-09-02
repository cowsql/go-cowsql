package logging

import (
	"fmt"
	"log/slog"

	"github.com/cowsql/go-cowsql/client"
)

// CowsqlLog redirects cowsql's logs to our own logger.
func CowsqlLog(l client.LogLevel, format string, a ...any) {
	format = fmt.Sprintf("Cowsql: %s", format)
	format = fmt.Sprintf(format, a...)
	switch l {
	case client.LogDebug:
		slog.Debug(format)
	case client.LogInfo:
		slog.Debug(format)
	case client.LogWarn:
		slog.Debug(format)
	case client.LogError:
		slog.Error(format)
	}
}
