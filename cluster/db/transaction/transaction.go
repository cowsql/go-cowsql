package transaction

import (
	"context"
	"database/sql"
)

// DB is a database executor capable of beginning a transaction.
type DB interface {
	BeginTx(ctx context.Context) (TX, error)
	DBTX
}

type dbtx struct {
	db DB
}

// Enable returns a DBTX from the given DB that begins a transaction on the first call to Exec, Prepare, or Query, if inside of a transaction.Do.
func Enable(db DB) DBTX {
	return dbtx{
		db: db,
	}
}

// ExecContext is a wrapper for the underlying ExecContext, but first attempts to open a transaction if called inside of transaction.Do.
func (t dbtx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db, err := t.getDBTX(ctx)
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

// Prepare is a wrapper for the underlying Prepare, but first attempts to open a transaction if called inside of transaction.Do.
func (t dbtx) Prepare(query string) (*sql.Stmt, error) {
	return t.db.PrepareContext(context.Background(), query)
}

// PrepareContext is a wrapper for the underlying PrepareContext, but first attempts to open a transaction if called inside of transaction.Do.
func (t dbtx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	db, err := t.getDBTX(ctx)
	if err != nil {
		return nil, err
	}

	return db.PrepareContext(ctx, query)
}

// QueryContext is a wrapper for the underlying QueryContext, but first attempts to open a transaction if called inside of transaction.Do.
func (t dbtx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := t.getDBTX(ctx)
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

// QueryRowContext is a wrapper for the underlying QueryRowContext, but first attempts to open a transaction if called inside of transaction.Do.
func (t dbtx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	db, err := t.getDBTX(ctx)
	if err != nil {
		// Workaround to create a *sql.Row with the private err field set to the
		// given error message.
		errDB, _ := sql.Open("cowsqlerrordriver", "")

		defer errDB.Close()

		return errDB.QueryRowContext(context.Background(), err.Error())
	}

	return db.QueryRowContext(ctx, query, args...)
}

func (t dbtx) getDBTX(ctx context.Context) (DBTX, error) {
	tc, ok := ctx.Value(tcKey{}).(*transactionContainer)
	if !ok {
		// No transaction started, use regular DB connection.
		return t.db, nil
	}

	tc.lock.Lock()
	defer tc.lock.Unlock()

	if tc.tx == nil {
		// Transaction requested, but no DB transaction started yet.
		tx, err := t.db.BeginTx(ctx)
		if err != nil {
			return nil, err
		}

		tc.tx = tx

		return tx, nil
	}

	// Transaction found, return it.
	return tc.tx, nil
}
