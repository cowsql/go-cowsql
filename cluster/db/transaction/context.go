package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

type tcKey struct{}

// TX is like DBTX but also implements Commit and Rollback.
type TX interface {
	DBTX
	transaction
}

// Transactor represents the implementation of a transaction capable entity.
type Transactor interface {
	// DBTX returns the underlying type that can exec statements or perform queries.
	DBTX() DBTX

	// MaxRetries returns the number of times a busy / failed transaction will be rolled back and retried before giving up.
	MaxRetries() int

	// OnTxStart performs initial setup preparing for the transaction.
	// Returns an updated transaction body, and a cleanup func.
	// If exclusive is true, then this transaction is blocking all others. This state should be cleared in the cleanup func.
	OnTxStart(exclusive bool, f func(ctx context.Context) error) (func(ctx context.Context) error, func())

	// OnTxStartForce is similar to OnTxStart but the transaction body contains an explicitly opened transaction.
	OnTxStartForce(exclusive bool, f func(ctx context.Context, tx TX) error) (func(ctx context.Context, tx TX) error, func())

	// EnterExclusive should block the opening of any transactions after called.
	// The Transactor's OnTxStart should handle clearing this state in its returned cleanup func.
	EnterExclusive() error
}

type transaction interface {
	Commit() error
	Rollback() error
}

// Do starts or reuses a transaction according to the provided context and transactor.
// Does not guarantee that a transaction has already opened when the transaction body first runs.
func Do(ctx context.Context, t Transactor, f func(ctx context.Context) error) error {
	return do(ctx, t, false, false, func(ctx context.Context, _ TX) error {
		return f(ctx)
	})
}

// DoExclusive starts or reuses a transaction with the exclusive flag set to true for OnTxStart, otherwise the same as Do.
func DoExclusive(ctx context.Context, t Transactor, f func(ctx context.Context) error) error {
	return do(ctx, t, true, false, func(ctx context.Context, _ TX) error {
		return f(ctx)
	})
}

// ForceTx starts or reuses a transaction, similar to Do, but immediately opens a transaction before calling OnTxStartForce, if one is not yet started.
func ForceTx(ctx context.Context, t Transactor, f func(context.Context, TX) error) error {
	db := t.DBTX()

	tx, ok := db.(TX)
	if ok {
		return f(ctx, tx)
	}

	return do(ctx, t, false, true, f)
}

func do(ctx context.Context, t Transactor, exclusive bool, force bool, f func(context.Context, TX) error) (err error) {
	if t == nil {
		return errors.New("Transactor has not been initialized")
	}

	ctx, trans := Begin(ctx)
	_, nestedTx := trans.(*noopTransactionContainer)

	doFunc := f

	if !nestedTx {
		if force {
			var cleanup func()

			doFunc, cleanup = t.OnTxStartForce(exclusive, func(ctx context.Context, tx TX) error {
				return f(ctx, tx)
			})

			defer cleanup()
		} else {
			// The transaction won't be opened until later so just pass nil.
			wrappedFunc, cleanup := t.OnTxStart(exclusive, func(ctx context.Context) error {
				return f(ctx, nil)
			})

			doFunc = func(ctx context.Context, _ TX) error {
				return wrappedFunc(ctx)
			}

			defer cleanup()
		}
	}

	defer func() {
		rollbackErr := trans.Rollback()
		if rollbackErr != nil {
			err = fmt.Errorf("Transaction rollback failed: %w, reason: %w", rollbackErr, err)
		}
	}()

	maxRetries := t.MaxRetries()

	// Only assign tx if force is true, else it will be implicitly created inside doFunc.
	forceTx := func(ctx context.Context) (TX, error) {
		if !force {
			return nil, nil //nolint:nilnil
		}

		dbtx, err := BeginDBTX(ctx, t.DBTX())
		if err != nil {
			return nil, err
		}

		tx, ok := dbtx.(TX)
		if !ok {
			return nil, errors.New("Failed to open a transaction")
		}

		return tx, nil
	}

	if !nestedTx && maxRetries > 0 {
		err = Retry(ctx, maxRetries, func(ctx context.Context) error {
			tx, err := forceTx(ctx)
			if err != nil {
				return err
			}

			reason := doFunc(ctx, tx)
			if reason != nil {
				err := Retry(context.Background(), maxRetries, func(_ context.Context) error { return trans.Rollback() })
				if err != nil {
					slog.Warn("Failed to rollback transaction after error", "reason", reason, "err", err)
				}
			}

			return reason
		})
	} else {
		var tx TX

		tx, err = forceTx(ctx)
		if err != nil {
			return err
		}

		err = doFunc(ctx, tx)
	}

	if err != nil {
		return err
	}

	err = trans.Commit()
	if err != nil {
		return fmt.Errorf("Failed commit transaction: %w", err)
	}

	return nil
}

// GetDBTX operates like BeginDBTX, but never errors. If a transaction is not open, and can't be opened, the underlying DB is returned.
func GetDBTX(ctx context.Context, db DBTX) DBTX {
	tx, err := BeginDBTX(ctx, db)
	if err != nil {
		return db
	}

	return tx
}

// BeginDBTX either begins a new TX (if one has not begun), returns the running TX, or returns the DB if not called within a transaction block.
func BeginDBTX(ctx context.Context, db DBTX) (DBTX, error) {
	if !IsActive(ctx) {
		return db, nil
	}

	tc, ok := ctx.Value(tcKey{}).(*transactionContainer)
	if ok {
		tc.lock.Lock()
		defer tc.lock.Unlock()

		// Transaction has started, return the underlying TX.
		if tc.tx != nil {
			return tc.tx, nil
		}

		internalDBTX, ok := db.(dbtx)
		if !ok {
			return nil, fmt.Errorf("Transaction container is invalid: %T", db)
		}

		// Transaction requested, but no DB transaction started yet.
		tx, err := internalDBTX.db.BeginTx(ctx)
		if err != nil {
			return nil, err
		}

		tc.tx = tx

		return tx, nil
	}

	// As of now this should never happen because the ctx will always contain a transaction container if not nil.
	return nil, errors.New("Unexpected transaction container")
}

// IsActive returns whether the context detects that it is a child context within a transaction block.
func IsActive(ctx context.Context) bool {
	existingTC := ctx.Value(tcKey{})

	return existingTC != nil
}

// Begin marks the context as having begun a transaction, if not already set.
func Begin(ctx context.Context) (context.Context, transaction) {
	isTC := IsActive(ctx)
	if isTC {
		return ctx, &noopTransactionContainer{}
	}

	tc := &transactionContainer{}

	return context.WithValue(ctx, tcKey{}, tc), tc
}

type transactionContainer struct {
	tx   TX
	lock sync.Mutex
}

var _ transaction = &transactionContainer{}

func (t *transactionContainer) Commit() error {
	if t.tx == nil {
		return nil
	}

	return t.tx.Commit()
}

func (t *transactionContainer) Rollback() error {
	if t.tx == nil {
		return nil
	}

	err := t.tx.Rollback()

	t.lock.Lock()
	t.tx = nil
	t.lock.Unlock()

	if !errors.Is(err, sql.ErrTxDone) {
		return err
	}

	return nil
}

type noopTransactionContainer struct{}

var _ transaction = noopTransactionContainer{}

func (n noopTransactionContainer) Commit() error {
	return nil
}

func (n noopTransactionContainer) Rollback() error {
	return nil
}
