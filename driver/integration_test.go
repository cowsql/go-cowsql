package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	cowsql "github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/driver"
	"github.com/cowsql/go-cowsql/logging"
)

// https://sqlite.org/rescode.html#constraint_unique
const SQLITE_CONSTRAINT_UNIQUE = 2067

var (
	requireNoError = assertNoError
	requireTrue    = assertTrue
	requireEqual   = assertEqual
)

func assertTrue(t *testing.T, ok bool) {
	t.Helper()
	if !ok {
		t.Fatal(ok)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if expected == nil || actual == nil {
		if expected != actual {
			t.Fatal(expected, actual)
		}
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatal(expected, actual)
	}
}

func assertEqualError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatal()
	}

	if err.Error() != msg {
		t.Fatal(err)
	}
}

func requireNil(t *testing.T, x interface{}) {
	t.Helper()

	if x != nil {
		v := reflect.ValueOf(x)
		switch v.Kind() {
		case reflect.Chan, reflect.Func,
			reflect.Interface, reflect.Map,
			reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
			if !v.IsNil() {
				t.Fatal(x)
			}
		}
	}
}

func requireNotNil(t *testing.T, x interface{}) {
	t.Helper()

	if x == nil {
		t.Fatal(x)
	}

	v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Chan, reflect.Func,
		reflect.Interface, reflect.Map,
		reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		if v.IsNil() {
			t.Fatal(x)
		}
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal()
	}
}

func TestIntegration_DatabaseSQL(t *testing.T) {
	db, _, cleanup := newDB(t, 3)
	defer cleanup()

	tx, err := db.Begin()
	requireNoError(t, err)

	_, err = tx.Exec(`
CREATE TABLE test  (n INT, s TEXT);
CREATE TABLE test2 (n INT, t DATETIME DEFAULT CURRENT_TIMESTAMP)
`)
	requireNoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n, s) VALUES(?, ?)")
	requireNoError(t, err)

	_, err = stmt.Exec(int64(123), "hello")
	requireNoError(t, err)

	requireNoError(t, stmt.Close())

	_, err = tx.Exec("INSERT INTO test2(n) VALUES(?)", int64(456))
	requireNoError(t, err)

	requireNoError(t, tx.Commit())

	tx, err = db.Begin()
	requireNoError(t, err)

	rows, err := tx.Query("SELECT n, s FROM test")
	requireNoError(t, err)

	for rows.Next() {
		var n int64
		var s string

		requireNoError(t, rows.Scan(&n, &s))

		assertEqual(t, int64(123), n)
		assertEqual(t, "hello", s)
	}

	requireNoError(t, rows.Err())
	requireNoError(t, rows.Close())

	rows, err = tx.Query("SELECT n, t FROM test2")
	requireNoError(t, err)

	for rows.Next() {
		var n int64
		var s time.Time

		requireNoError(t, rows.Scan(&n, &s))

		assertEqual(t, int64(456), n)
	}

	requireNoError(t, rows.Err())
	requireNoError(t, rows.Close())

	requireNoError(t, tx.Rollback())
}

func TestIntegration_ConstraintError(t *testing.T) {
	db, _, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT, UNIQUE (n))")
	requireNoError(t, err)

	_, err = db.Exec("INSERT INTO test (n) VALUES (1)")
	requireNoError(t, err)

	_, err = db.Exec("INSERT INTO test (n) VALUES (1)")
	if err, ok := err.(driver.Error); ok {
		assertEqual(t, SQLITE_CONSTRAINT_UNIQUE, err.Code)
		assertEqual(t, "UNIQUE constraint failed: test.n", err.Message)
	} else {
		t.Fatalf("expected diver error, got %+v", err)
	}
}

func TestIntegration_ExecBindError(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := db.ExecContext(ctx, "CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO test(n) VALUES(1)", 1)
	assertEqualError(t, err, "bind parameters")
}

func TestIntegration_QueryBindError(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := db.QueryContext(ctx, "SELECT 1", 1)
	assertEqualError(t, err, "bind parameters")
}

func TestIntegration_LargeQuery(t *testing.T) {
	db, _, cleanup := newDB(t, 3)
	defer cleanup()

	tx, err := db.Begin()
	requireNoError(t, err)

	_, err = tx.Exec("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n) VALUES(?)")
	requireNoError(t, err)

	for i := 0; i < 512; i++ {
		_, err = stmt.Exec(int64(i))
		requireNoError(t, err)
	}

	requireNoError(t, stmt.Close())

	requireNoError(t, tx.Commit())

	tx, err = db.Begin()
	requireNoError(t, err)

	rows, err := tx.Query("SELECT n FROM test")
	requireNoError(t, err)

	columns, err := rows.Columns()
	requireNoError(t, err)

	assertEqual(t, []string{"n"}, columns)

	count := 0
	for i := 0; rows.Next(); i++ {
		var n int64

		requireNoError(t, rows.Scan(&n))

		assertEqual(t, int64(i), n)
		count++
	}

	requireNoError(t, rows.Err())
	requireNoError(t, rows.Close())

	assertEqual(t, count, 512)

	requireNoError(t, tx.Rollback())
}

// Build a 2-node cluster, kill one node and recover the other.
func TestIntegration_Recover(t *testing.T) {
	db, helpers, cleanup := newDB(t, 2)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	helpers[0].Close()
	helpers[1].Close()

	helpers[0].Create()

	infos := []client.NodeInfo{{ID: 1, Address: "@1"}}
	requireNoError(t, helpers[0].Node.Recover(infos))

	helpers[0].Start()

	// FIXME: this is necessary otherwise the INSERT below fails with "no
	// such table", because the replication hooks are not triggered and the
	// barrier is not applied.
	_, err = db.Exec("CREATE TABLE test2 (n INT)")
	requireNoError(t, err)

	_, err = db.Exec("INSERT INTO test(n) VALUES(1)")
	requireNoError(t, err)
}

// The db.Ping() method can be used to wait until there is a stable leader.
func TestIntegration_PingOnlyWorksOnceLeaderElected(t *testing.T) {
	db, helpers, cleanup := newDB(t, 2)
	defer cleanup()

	helpers[0].Close()

	// Ping returns an error, since the cluster is not available.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assertError(t, db.PingContext(ctx))

	helpers[0].Create()
	helpers[0].Start()

	// Ping now returns no error, since the cluster is available.
	assertNoError(t, db.Ping())

	// If leadership is lost after the first successful call, Ping() still
	// returns no error.
	helpers[0].Close()
	assertNoError(t, db.Ping())
}

func TestIntegration_HighAvailability(t *testing.T) {
	db, helpers, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	// Shutdown all three nodes.
	helpers[0].Close()
	helpers[1].Close()
	helpers[2].Close()

	// Restart two of them.
	helpers[1].Create()
	helpers[2].Create()
	helpers[1].Start()
	helpers[2].Start()

	// Give the cluster a chance to establish a quorom
	time.Sleep(2 * time.Second)

	_, err = db.Exec("INSERT INTO test(n) VALUES(1)")
	requireNoError(t, err)
}

func TestIntegration_LeadershipTransfer(t *testing.T) {
	db, helpers, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	cli := helpers[0].Client()
	requireNoError(t, cli.Transfer(context.Background(), 2))

	_, err = db.Exec("INSERT INTO test(n) VALUES(1)")
	requireNoError(t, err)
}

func TestIntegration_LeadershipTransfer_Tx(t *testing.T) {
	db, helpers, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	cli := helpers[0].Client()
	requireNoError(t, cli.Transfer(context.Background(), 2))

	tx, err := db.Begin()
	requireNoError(t, err)

	_, err = tx.Query("SELECT * FROM test")
	requireNoError(t, err)

	requireNoError(t, tx.Commit())
}

func TestOptions(t *testing.T) {
	// make sure applying all options doesn't break anything
	store := client.NewInmemNodeStore()
	log := logging.Test(t)
	_, err := driver.New(
		store,
		driver.WithLogFunc(log),
		driver.WithContext(context.Background()),
		driver.WithConnectionTimeout(15*time.Second),
		driver.WithContextTimeout(2*time.Second),
		driver.WithConnectionBackoffFactor(50*time.Millisecond),
		driver.WithConnectionBackoffCap(1*time.Second),
		driver.WithAttemptTimeout(5*time.Second),
		driver.WithRetryLimit(0),
	)
	requireNoError(t, err)
}

func newDB(t *testing.T, n int) (*sql.DB, []*nodeHelper, func()) {
	infos := make([]client.NodeInfo, n)
	for i := range infos {
		infos[i].ID = uint64(i + 1)
		infos[i].Address = fmt.Sprintf("@%d", infos[i].ID)
		infos[i].Role = client.Voter
	}
	return newDBWithInfos(t, infos)
}

func newDBWithInfos(t *testing.T, infos []client.NodeInfo) (*sql.DB, []*nodeHelper, func()) {
	helpers, helpersCleanup := newNodeHelpers(t, infos)

	store := client.NewInmemNodeStore()

	requireNoError(t, store.Set(context.Background(), infos))

	log := logging.Test(t)

	driver, err := driver.New(store, driver.WithLogFunc(log))
	requireNoError(t, err)

	driverName := fmt.Sprintf("cowsql-integration-test-%d", driversCount)
	sql.Register(driverName, driver)

	driversCount++

	db, err := sql.Open(driverName, "test.db")
	requireNoError(t, err)

	cleanup := func() {
		requireNoError(t, db.Close())
		helpersCleanup()
	}

	return db, helpers, cleanup
}

func registerDriver(driver *driver.Driver) string {
	name := fmt.Sprintf("cowsql-integration-test-%d", driversCount)
	sql.Register(name, driver)
	driversCount++
	return name
}

type nodeHelper struct {
	t       *testing.T
	ID      uint64
	Address string
	Dir     string
	Node    *cowsql.Node
}

func newNodeHelper(t *testing.T, id uint64, address string) *nodeHelper {
	h := &nodeHelper{
		t:       t,
		ID:      id,
		Address: address,
	}

	h.Dir, _ = newDir(t)

	h.Create()
	h.Start()

	return h
}

func (h *nodeHelper) Client() *client.Client {
	client, err := client.New(context.Background(), h.Node.BindAddress())
	requireNoError(h.t, err)
	return client
}

func (h *nodeHelper) Create() {
	var err error
	requireNil(h.t, h.Node)
	h.Node, err = cowsql.New(h.ID, h.Address, h.Dir, cowsql.WithBindAddress(h.Address))
	requireNoError(h.t, err)
}

func (h *nodeHelper) Start() {
	requireNotNil(h.t, h.Node)
	requireNoError(h.t, h.Node.Start())
}

func (h *nodeHelper) Close() {
	requireNotNil(h.t, h.Node)
	requireNoError(h.t, h.Node.Close())
	h.Node = nil
}

func (h *nodeHelper) cleanup() {
	if h.Node != nil {
		h.Close()
	}
	requireNoError(h.t, os.RemoveAll(h.Dir))
}

func newNodeHelpers(t *testing.T, infos []client.NodeInfo) ([]*nodeHelper, func()) {
	t.Helper()

	n := len(infos)
	helpers := make([]*nodeHelper, n)

	for i, info := range infos {
		helpers[i] = newNodeHelper(t, info.ID, info.Address)

		if i > 0 {
			client := helpers[0].Client()
			defer client.Close()

			requireNoError(t, client.Add(context.Background(), infos[i]))
		}
	}

	cleanup := func() {
		for _, helper := range helpers {
			helper.cleanup()
		}
	}

	return helpers, cleanup
}

var driversCount = 0

func TestIntegration_ColumnTypeName(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT, UNIQUE (n))")
	requireNoError(t, err)

	_, err = db.Exec("INSERT INTO test (n) VALUES (1)")
	requireNoError(t, err)

	rows, err := db.Query("SELECT n FROM test")
	requireNoError(t, err)
	defer rows.Close()

	types, err := rows.ColumnTypes()
	requireNoError(t, err)

	assertEqual(t, "INTEGER", types[0].DatabaseTypeName())

	requireTrue(t, rows.Next())
	var n int64
	err = rows.Scan(&n)
	requireNoError(t, err)

	assertEqual(t, int64(1), n)
}

func TestIntegration_SqlNullTime(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (tm DATETIME)")
	requireNoError(t, err)

	// Insert sql.NullTime into DB
	var t1 sql.NullTime
	res, err := db.Exec("INSERT INTO test (tm) VALUES (?)", t1)
	requireNoError(t, err)

	n, err := res.RowsAffected()
	requireNoError(t, err)
	if n != 1 {
		t.Fatal(n, 1)
	}

	// Retrieve inserted sql.NullTime from DB
	row := db.QueryRow("SELECT tm FROM test LIMIT 1")
	var t2 sql.NullTime
	err = row.Scan(&t2)
	requireNoError(t, err)

	assertEqual(t, t1, t2)
}
