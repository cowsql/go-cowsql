// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver_test

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"strings"
	"testing"

	cowsql "github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	cowsqldriver "github.com/cowsql/go-cowsql/driver"
	"github.com/cowsql/go-cowsql/logging"
)

func TestDriver_Open(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	requireNoError(t, err)

	assertNoError(t, conn.Close())
}

func TestDriver_Prepare(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	assertEqual(t, 0, stmt.NumInput())

	assertNoError(t, conn.Close())
}

func TestConn_Exec(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	requireNoError(t, err)

	result, err := execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	requireNoError(t, err)

	lastInsertID, err := result.LastInsertId()
	requireNoError(t, err)

	assertEqual(t, lastInsertID, int64(1))

	rowsAffected, err := result.RowsAffected()
	requireNoError(t, err)

	assertEqual(t, rowsAffected, int64(1))

	assertNoError(t, conn.Close())
}

func TestConn_Query(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	requireNoError(t, err)

	_, err = execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	requireNoError(t, err)

	queryer := conn.(driver.Queryer)

	_, err = queryer.Query("SELECT n FROM test", nil)
	requireNoError(t, err)

	assertNoError(t, conn.Close())
}

func TestConn_QueryRow(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	requireNoError(t, err)

	_, err = execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	requireNoError(t, err)

	_, err = execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	requireNoError(t, err)

	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("SELECT n FROM test", nil)
	requireNoError(t, err)

	values := make([]driver.Value, 1)
	requireNoError(t, rows.Next(values))

	requireNoError(t, rows.Close())

	assertNoError(t, conn.Close())
}

func TestConn_QueryBlob(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (data BLOB)", nil)
	requireNoError(t, err)

	values := []driver.Value{
		[]byte{'a', 'b', 'c'},
	}
	_, err = execer.Exec("INSERT INTO test(data) VALUES(?)", values)
	requireNoError(t, err)

	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("SELECT data FROM test", nil)
	requireNoError(t, err)

	assertEqual(t, rows.Columns(), []string{"data"})

	values = make([]driver.Value, 1)
	requireNoError(t, rows.Next(values))

	assertEqual(t, []byte{'a', 'b', 'c'}, values[0])

	assertNoError(t, conn.Close())
}

func TestStmt_Exec(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	values := []driver.Value{
		int64(1),
	}

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(?)")
	requireNoError(t, err)

	result, err := stmt.Exec(values)
	requireNoError(t, err)

	lastInsertID, err := result.LastInsertId()
	requireNoError(t, err)

	assertEqual(t, lastInsertID, int64(1))

	rowsAffected, err := result.RowsAffected()
	requireNoError(t, err)

	assertEqual(t, rowsAffected, int64(1))

	requireNoError(t, stmt.Close())

	assertNoError(t, conn.Close())
}

func TestStmt_ExecManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES " + strings.Repeat("(?), ", 299) + " (?)")
	requireNoError(t, err)

	values := make([]driver.Value, 300)
	for i := range values {
		values[i] = int64(1)
	}
	_, err = stmt.Exec(values)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())
	assertNoError(t, conn.Close())
}

func TestStmt_Query(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(-123)")
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("SELECT n FROM test")
	requireNoError(t, err)

	rows, err := stmt.Query(nil)
	requireNoError(t, err)

	assertEqual(t, rows.Columns(), []string{"n"})

	values := make([]driver.Value, 1)
	requireNoError(t, rows.Next(values))

	assertEqual(t, int64(-123), values[0])

	requireEqual(t, io.EOF, rows.Next(values))

	requireNoError(t, stmt.Close())

	assertNoError(t, conn.Close())
}

func TestStmt_QueryManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("SELECT n FROM test WHERE n IN (" + strings.Repeat("?, ", 299) + " ?)")
	requireNoError(t, err)

	values := make([]driver.Value, 300)
	for i := range values {
		values[i] = int64(1)
	}
	_, err = stmt.Query(values)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())
	assertNoError(t, conn.Close())
}

func TestConn_QueryParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT, t TEXT)", nil)
	requireNoError(t, err)

	_, err = execer.Exec(`
INSERT INTO test (n,t) VALUES (1,'a');
INSERT INTO test (n,t) VALUES (2,'a');
INSERT INTO test (n,t) VALUES (2,'b');
INSERT INTO test (n,t) VALUES (3,'b');
`,
		nil)
	requireNoError(t, err)

	values := []driver.Value{
		int64(1),
		"a",
	}

	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("SELECT n, t FROM test WHERE n > ? AND t = ?", values)
	requireNoError(t, err)

	assertEqual(t, rows.Columns()[0], "n")

	values = make([]driver.Value, 2)
	requireNoError(t, rows.Next(values))

	assertEqual(t, int64(2), values[0])
	assertEqual(t, "a", values[1])

	requireEqual(t, io.EOF, rows.Next(values))

	assertNoError(t, conn.Close())
}

func TestConn_QueryManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	requireNoError(t, err)

	values := make([]driver.Value, 300)
	for i := range values {
		values[i] = int64(1)
	}
	queryer := conn.(driver.Queryer)
	_, err = queryer.Query("SELECT n FROM test WHERE n IN ("+strings.Repeat("?, ", 299)+" ?)", values)
	requireNoError(t, err)

	assertNoError(t, conn.Close())
}

func TestConn_ExecManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	requireNoError(t, err)

	values := make([]driver.Value, 300)
	for i := range values {
		values[i] = int64(1)
	}

	_, err = execer.Exec("INSERT INTO test(n) VALUES "+strings.Repeat("(?), ", 299)+" (?)", values)
	requireNoError(t, err)

	assertNoError(t, conn.Close())
}

func Test_ColumnTypesEmpty(t *testing.T) {
	t.Skip("this currently fails if the result set is empty, is cowsql skipping the header if empty set?")
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("SELECT n FROM test")
	requireNoError(t, err)

	rows, err := stmt.Query(nil)
	requireNoError(t, err)

	requireNoError(t, err)
	rowTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	requireTrue(t, ok)

	typeName := rowTypes.ColumnTypeDatabaseTypeName(0)
	assertEqual(t, "INTEGER", typeName)

	requireNoError(t, stmt.Close())

	assertNoError(t, conn.Close())
}

func Test_ColumnTypesExists(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(-123)")
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	stmt, err = conn.Prepare("SELECT n FROM test")
	requireNoError(t, err)

	rows, err := stmt.Query(nil)
	requireNoError(t, err)

	requireNoError(t, err)
	rowTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	requireTrue(t, ok)

	typeName := rowTypes.ColumnTypeDatabaseTypeName(0)
	assertEqual(t, "INTEGER", typeName)

	requireNoError(t, stmt.Close())
	assertNoError(t, conn.Close())
}

// ensure column types data is available
// even after the last row of the query
func Test_ColumnTypesEnd(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = conn.Begin()
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(-123)")
	requireNoError(t, err)

	_, err = stmt.Exec(nil)
	requireNoError(t, err)

	stmt, err = conn.Prepare("SELECT n FROM test")
	requireNoError(t, err)

	rows, err := stmt.Query(nil)
	requireNoError(t, err)

	requireNoError(t, err)
	rowTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	requireTrue(t, ok)

	typeName := rowTypes.ColumnTypeDatabaseTypeName(0)
	assertEqual(t, "INTEGER", typeName)

	values := make([]driver.Value, 1)
	requireNoError(t, rows.Next(values))

	assertEqual(t, int64(-123), values[0])

	requireEqual(t, io.EOF, rows.Next(values))

	// despite EOF we should have types cached
	typeName = rowTypes.ColumnTypeDatabaseTypeName(0)
	assertEqual(t, "INTEGER", typeName)

	requireNoError(t, stmt.Close())
	assertNoError(t, conn.Close())
}

func Test_ZeroColumns(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	requireNoError(t, err)
	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("CREATE TABLE foo (bar INTEGER)", []driver.Value{})
	requireNoError(t, err)
	values := []driver.Value{}
	requireEqual(t, io.EOF, rows.Next(values))

	requireNoError(t, conn.Close())
}

func newDriver(t *testing.T) (*cowsqldriver.Driver, func()) {
	t.Helper()

	_, cleanup := newNode(t)

	store := newStore(t, "@1")

	log := logging.Test(t)

	driver, err := cowsqldriver.New(store, cowsqldriver.WithLogFunc(log))
	requireNoError(t, err)

	return driver, cleanup
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, address string) client.NodeStore {
	t.Helper()

	store := client.NewInmemNodeStore()
	server := client.NodeInfo{Address: address}
	requireNoError(t, store.Set(context.Background(), []client.NodeInfo{server}))

	return store
}

func newNode(t *testing.T) (*cowsql.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	server, err := cowsql.New(uint64(1), "@1", dir, cowsql.WithBindAddress("@1"))
	requireNoError(t, err)

	err = server.Start()
	requireNoError(t, err)

	cleanup := func() {
		requireNoError(t, server.Close())
		dirCleanup()
	}

	return server, cleanup
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "cowsql-replication-test-")
	assertNoError(t, err)

	cleanup := func() {
		_, err := os.Stat(dir)
		if err != nil {
			assertTrue(t, os.IsNotExist(err))
		} else {
			assertNoError(t, os.RemoveAll(dir))
		}
	}

	return dir, cleanup
}
