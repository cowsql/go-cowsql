//go:build !nosqlite3 && !darwin

package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/cowsql/go-cowsql/cluster/db/transaction"
)

// Function to scan a single row.
type scanFunc func(*sql.Rows) error

// Dest is a function that is expected to return the objects to pass to the
// 'dest' argument of sql.Rows.Scan(). It is invoked by SelectObjects once per
// yielded row, and it will be passed the index of the row being scanned.
type Dest func(scan func(dest ...any) error) error

// SelectObjects executes a statement which must yield rows with a specific
// columns schema. It invokes the given Dest hook for each yielded row.
func SelectObjects(ctx context.Context, tx transaction.DBTX, stmt string, rowFunc Dest, args ...any) error {
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err = rowFunc(rows.Scan)
		if err != nil {
			return err
		}
	}

	return rows.Err()
}

// DeleteObject removes the row identified by the given ID. The given table
// must have a primary key column called 'id'.
//
// It returns a flag indicating if a matching row was actually found and
// deleted or not.
func DeleteObject(ctx context.Context, tx transaction.DBTX, table string, id int64) (bool, error) {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE id=?", table)

	result, err := tx.ExecContext(ctx, stmt, id)
	if err != nil {
		return false, err
	}

	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if n > 1 {
		return true, errors.New("more than one row was deleted")
	}

	return n == 1, nil
}

// UpsertObject inserts or replaces a new row with the given column values, to
// the given table using columns order. For example:
//
// UpsertObject(tx, "cars", []string{"id", "brand"}, []any{1, "ferrari"})
//
// The number of elements in 'columns' must match the one in 'values'.
func UpsertObject(ctx context.Context, tx transaction.DBTX, table string, columns []string, values []any) (int64, error) {
	n := len(columns)
	if n == 0 {
		return -1, errors.New("columns length is zero")
	}

	if n != len(values) {
		return -1, errors.New("columns length does not match values length")
	}

	stmt := fmt.Sprintf(
		"INSERT OR REPLACE INTO %s (%s) VALUES %s",
		table, strings.Join(columns, ", "), Params(n),
	)

	result, err := tx.ExecContext(ctx, stmt, values...)
	if err != nil {
		return -1, fmt.Errorf("insert or replaced row: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return -1, fmt.Errorf("get last inserted ID: %w", err)
	}

	return id, nil
}

// SelectStrings executes a statement which must yield rows with a single string
// column. It returns the list of column values.
func SelectStrings(ctx context.Context, tx transaction.DBTX, query string, args ...any) ([]string, error) {
	values := []string{}
	scan := func(rows *sql.Rows) error {
		var value string

		err := rows.Scan(&value)
		if err != nil {
			return err
		}

		values = append(values, value)

		return nil
	}

	err := scanSingleColumn(ctx, tx, query, args, scan)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// Execute the given query and ensure that it yields rows with a single column
// of the given database type. For every row yielded, execute the given
// scanner.
func scanSingleColumn(ctx context.Context, tx transaction.DBTX, query string, args []any, scan scanFunc) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err := scan(rows)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

// Params returns a parameters expression with the given number of '?'
// placeholders. E.g. Params(2) -> "(?, ?)". Useful for IN and VALUES
// expressions.
func Params(n int) string {
	tokens := make([]string, n)
	for i := range n {
		tokens[i] = "?"
	}

	return fmt.Sprintf("(%s)", strings.Join(tokens, ", "))
}

// Count returns the number of rows in the given table.
func Count(ctx context.Context, tx transaction.DBTX, table string, where string, args ...any) (int, error) {
	stmt := "SELECT COUNT(*) FROM " + table
	if where != "" {
		stmt += " WHERE " + where
	}

	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return -1, err
	}

	defer rows.Close()

	// Ensure we read one and only one row.
	if !rows.Next() {
		return -1, errors.New("no rows returned")
	}

	var count int

	err = rows.Scan(&count)
	if err != nil {
		return -1, errors.New("failed to scan count column")
	}

	if rows.Next() {
		return -1, errors.New("more than one row returned")
	}

	err = rows.Err()
	if err != nil {
		return -1, err
	}

	return count, nil
}
