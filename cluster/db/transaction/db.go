package transaction

import (
	"context"
	"database/sql"
)

type Preparer interface {
	DBTX
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
