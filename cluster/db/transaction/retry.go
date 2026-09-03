package transaction

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"

	"github.com/cowsql/go-cowsql/cluster/internal/util/api"
	"github.com/cowsql/go-cowsql/driver"
)

// Retry wraps a function that interacts with the database, and retries it in
// case a transient error is hit.
//
// This should by typically used to wrap transactions.
func Retry(ctx context.Context, maxRetries int, f func(ctx context.Context) error) error {
	var err error
	for i := range maxRetries {
		err = f(ctx)
		if err == nil {
			// The function succeeded, we're done here.
			break
		}

		if errors.Is(err, context.Canceled) {
			// The function was canceled, don't retry.
			break
		}

		// No point in re-trying or logging a no-row or not found error.
		if errors.Is(err, sql.ErrNoRows) || api.StatusErrorCheck(err, http.StatusNotFound) {
			break
		}

		// FIXME: Perform one retry attempt to cover issues connecting to the cowsql leader.
		if i == 0 && i < maxRetries && errors.Is(err, context.DeadlineExceeded) {
			slog.Debug("Database error, retrying", "attempt", i, "err", err)
			time.Sleep(jitterDeviation(0.8, 100*time.Millisecond))
			continue
		}

		// Process actual errors.
		if !IsRetriableError(err) {
			slog.Debug("Database error", "err", err)
			break
		}

		if i == maxRetries-1 {
			slog.Warn("Database error, giving up", "attempt", i, "err", err)
			break
		}

		slog.Debug("Database error, retrying", "attempt", i, "err", err)
		time.Sleep(jitterDeviation(0.8, 100*time.Millisecond))
	}

	return err
}

func jitterDeviation(factor float64, duration time.Duration) time.Duration {
	floor := int64(math.Floor(float64(duration) * (1 - factor)))
	ceil := int64(math.Ceil(float64(duration) * (1 + factor)))
	return time.Duration(rand.Int64N(ceil-floor) + floor)
}

// IsRetriableError returns true if the given error might be transient and the
// interaction can be safely retried.
func IsRetriableError(err error) bool {
	var dErr *driver.Error

	if errors.As(err, &dErr) && dErr.Code == driver.ErrBusy {
		return true
	}

	if errors.Is(err, sqlite3.ErrLocked) || errors.Is(err, sqlite3.ErrBusy) {
		return true
	}

	// Unwrap errors one at a time.
	for ; err != nil; err = errors.Unwrap(err) {
		if strings.Contains(err.Error(), "database is locked") {
			return true
		}

		if strings.Contains(err.Error(), "cannot start a transaction within a transaction") {
			return true
		}

		if strings.Contains(err.Error(), "bad connection") {
			return true
		}

		if strings.Contains(err.Error(), "checkpoint in progress") {
			return true
		}
	}

	return false
}
