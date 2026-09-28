package logging

import (
	"fmt"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster/internal/logger"
)

// CowsqlLog redirects cowsql's logs to our own logger.
func CowsqlLog(l client.LogLevel, format string, a ...any) {
	format = "Cowsql: " + format
	format = fmt.Sprintf(format, a...)

	switch l {
	case client.LogDebug, client.LogInfo, client.LogWarn:
		logger.Log().Debug(format)
	case client.LogError:
		logger.Log().Error(format)
	}
}
