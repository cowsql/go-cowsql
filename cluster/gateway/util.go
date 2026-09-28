package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"time"

	"github.com/cowsql/go-cowsql/cluster/internal/logger"
)

// writeJSON encodes the body as JSON and sends it back to the client
// Accepts optional debugLogger that activates debug logging if non-nil.
func writeJSON(w http.ResponseWriter, body any) error {
	var output io.Writer = w

	enc := json.NewEncoder(output)
	enc.SetEscapeHTML(false)
	err := enc.Encode(body)

	return err
}

// closeOrLog executes the closer with a timeout as queries
// stuck on an unreachable cluster can block it indefinitely.
func closeOrLog(msg string, timeout time.Duration, closer func() error) {
	done := make(chan struct{})

	go func() {
		err := closer()
		if err != nil {
			logger.Log().Debug("Failed to run closer", slog.String("message", msg), slog.Any("error", err))
		}

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		buf := make([]byte, 1024*1024)
		n := runtime.Stack(buf, true)
		logger.Log().Warn("Timed out running closer", slog.String("timeout", timeout.String()), slog.String("message", msg), slog.String("goroutines", string(buf[:n])))
	}
}
