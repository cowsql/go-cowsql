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

package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"syscall"
	"time"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/internal/protocol"
)

// Driver perform queries against a cowsql server.
type Driver struct {
	log               client.LogFunc   // Log function to use
	store             client.NodeStore // Holds addresses of cowsql servers
	context           context.Context  // Global cancellation context
	connectionTimeout time.Duration    // Max time to wait for a new connection
	contextTimeout    time.Duration    // Default client context timeout.
	clientConfig      protocol.Config  // Configuration for cowsql client instances
	tracing           client.LogLevel  // Whether to trace statements
}

// Error is returned in case of database errors.
type Error = protocol.Error

// Error codes. Values here mostly overlap with native SQLite codes.
const (
	ErrBusy                = 5
	ErrBusyRecovery        = 5 | (1 << 8)
	ErrBusySnapshot        = 5 | (2 << 8)
	errIoErr               = 10
	errIoErrNotLeader      = errIoErr | 40<<8
	errIoErrLeadershipLost = errIoErr | (41 << 8)
	errNotFound            = 12

	// Legacy error codes before version-3.32.1+replication4. Kept here
	// for backward compatibility, but should eventually be dropped.
	errIoErrNotLeaderLegacy      = errIoErr | 32<<8
	errIoErrLeadershipLostLegacy = errIoErr | (33 << 8)
)

// Option can be used to tweak driver parameters.
type Option func(*options)

// NodeStore is a convenience alias of client.NodeStore.
type NodeStore = client.NodeStore

// NodeInfo is a convenience alias of client.NodeInfo.
type NodeInfo = client.NodeInfo

// DefaultNodeStore is a convenience alias of client.DefaultNodeStore.
var DefaultNodeStore = client.DefaultNodeStore

// WithLogFunc sets a custom logging function.
func WithLogFunc(log client.LogFunc) Option {
	return func(options *options) {
		options.Log = log
	}
}

// DialFunc is a function that can be used to establish a network connection
// with a cowsql node.
type DialFunc = protocol.DialFunc

// WithDialFunc sets a custom dial function.
func WithDialFunc(dial DialFunc) Option {
	return func(options *options) {
		options.Dial = protocol.DialFunc(dial)
	}
}

// WithConnectionTimeout sets the connection timeout.
//
// If not used, the default is 5 seconds.
//
// DEPRECATED: Connection cancellation is supported via the driver.Connector
// interface, which is used internally by the stdlib sql package.
func WithConnectionTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.ConnectionTimeout = timeout
	}
}

// WithConnectionBackoffFactor sets the exponential backoff factor for retrying
// failed connection attempts.
//
// If not used, the default is 100 milliseconds.
func WithConnectionBackoffFactor(factor time.Duration) Option {
	return func(options *options) {
		options.ConnectionBackoffFactor = factor
	}
}

// WithConnectionBackoffCap sets the maximum connection retry backoff value,
// (regardless of the backoff factor) for retrying failed connection attempts.
//
// If not used, the default is 1 second.
func WithConnectionBackoffCap(cap time.Duration) Option {
	return func(options *options) {
		options.ConnectionBackoffCap = cap
	}
}

// WithAttemptTimeout sets the timeout for each individual connection attempt.
//
// The Connector.Connect() and Driver.Open() methods try to find the current
// leader among the servers in the store that was passed to New(). Each time
// they attempt to probe an individual server for leadership this timeout will
// apply, so a server which accepts the connection but it's then unresponsive
// won't block the line.
//
// If not used, the default is 15 seconds.
func WithAttemptTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.AttemptTimeout = timeout
	}
}

// WithRetryLimit sets the maximum number of connection retries.
//
// If not used, the default is 0 (unlimited retries)
func WithRetryLimit(limit uint) Option {
	return func(options *options) {
		options.RetryLimit = limit
	}
}

// WithContext sets a global cancellation context.
//
// DEPRECATED: This API is no a no-op. Users should explicitly pass a context
// if they wish to cancel their requests.
func WithContext(context context.Context) Option {
	return func(options *options) {
		options.Context = context
	}
}

// WithContextTimeout sets the default client context timeout for DB.Begin()
// when no context deadline is provided.
//
// DEPRECATED: Users should use db APIs that support contexts if they wish to
// cancel their requests.
func WithContextTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.ContextTimeout = timeout
	}
}

// WithTracing will emit a log message at the given level every time a
// statement gets executed.
func WithTracing(level client.LogLevel) Option {
	return func(options *options) {
		options.Tracing = level
	}
}

// NewDriver creates a new cowsql driver, which also implements the
// driver.Driver interface.
func New(store client.NodeStore, options ...Option) (*Driver, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}

	driver := &Driver{
		log:               o.Log,
		store:             store,
		context:           o.Context,
		connectionTimeout: o.ConnectionTimeout,
		contextTimeout:    o.ContextTimeout,
		tracing:           o.Tracing,
		clientConfig: protocol.Config{
			Dial:           o.Dial,
			AttemptTimeout: o.AttemptTimeout,
			BackoffFactor:  o.ConnectionBackoffFactor,
			BackoffCap:     o.ConnectionBackoffCap,
			RetryLimit:     o.RetryLimit,
		},
	}

	return driver, nil
}

// Hold configuration options for a cowsql driver.
type options struct {
	Log                     client.LogFunc
	Dial                    protocol.DialFunc
	AttemptTimeout          time.Duration
	ConnectionTimeout       time.Duration
	ContextTimeout          time.Duration
	ConnectionBackoffFactor time.Duration
	ConnectionBackoffCap    time.Duration
	RetryLimit              uint
	Context                 context.Context
	Tracing                 client.LogLevel
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	return &options{
		Log:     client.DefaultLogFunc,
		Dial:    client.DefaultDialFunc,
		Tracing: client.LogNone,
	}
}

// A Connector represents a driver in a fixed configuration and can create any
// number of equivalent Conns for use by multiple goroutines.
type Connector struct {
	uri    string
	driver *Driver
}

// Connect returns a connection to the database.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.driver.context != nil {
		ctx = c.driver.context
	}

	if c.driver.connectionTimeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, c.driver.connectionTimeout)
		defer cancel()
	}

	// TODO: generate a client ID.
	connector := protocol.NewConnector(0, c.driver.store, c.driver.clientConfig, c.driver.log)

	conn := &Conn{
		log:            c.driver.log,
		contextTimeout: c.driver.contextTimeout,
		tracing:        c.driver.tracing,
	}

	var err error
	conn.protocol, err = connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create cowsql connection: %w", err)
	}

	conn.request.Init(4096)
	conn.response.Init(4096)

	protocol.EncodeOpen(&conn.request, c.uri, 0, "volatile")

	if err := conn.protocol.Call(ctx, &conn.request, &conn.response); err != nil {
		conn.protocol.Close()
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	conn.id, err = protocol.DecodeDb(&conn.response)
	if err != nil {
		conn.protocol.Close()
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return conn, nil
}

// Driver returns the underlying Driver of the Connector,
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	connector := &Connector{
		uri:    name,
		driver: d,
	}
	return connector, nil
}

// Open establishes a new connection to a SQLite database on the cowsql server.
//
// The given name must be a pure file name without any directory segment,
// cowsql will connect to a database with that name in its data directory.
//
// Query parameters are always valid except for "mode=memory".
//
// If this node is not the leader, or the leader is unknown an ErrNotLeader
// error is returned.
func (d *Driver) Open(uri string) (driver.Conn, error) {
	connector, err := d.OpenConnector(uri)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.Background())
}

// SetContextTimeout sets the default client timeout when no context deadline
// is provided.
//
// DEPRECATED: This API is no a no-op. Users should explicitly pass a context
// if they wish to cancel their requests, or use the WithContextTimeout option.
func (d *Driver) SetContextTimeout(timeout time.Duration) {}

// ErrNoAvailableLeader is returned as root cause of Open() if there's no
// leader available in the cluster.
var ErrNoAvailableLeader = protocol.ErrNoAvailableLeader

// Conn implements the sql.Conn interface.
type Conn struct {
	log            client.LogFunc
	protocol       *protocol.Protocol
	request        protocol.Message
	response       protocol.Message
	id             uint32 // Database ID.
	contextTimeout time.Duration
	tracing        client.LogLevel
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement, it must not store the
// context within the statement itself.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt := &Stmt{
		protocol: c.protocol,
		request:  &c.request,
		response: &c.response,
		log:      c.log,
		tracing:  c.tracing,
	}

	protocol.EncodePrepare(&c.request, uint64(c.id), query)

	var start time.Time
	if c.tracing != client.LogNone {
		start = time.Now()
	}
	err := c.protocol.Call(ctx, &c.request, &c.response)
	if c.tracing != client.LogNone {
		c.log(c.tracing, "%.3fs request prepared: %q", time.Since(start).Seconds(), query)
	}
	if err != nil {
		return nil, driverError(c.log, err)
	}

	stmt.db, stmt.id, stmt.params, err = protocol.DecodeStmt(&c.response)
	if err != nil {
		return nil, driverError(c.log, err)
	}

	if c.tracing != client.LogNone {
		stmt.sql = query
	}

	return stmt, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// ExecContext is an optional interface that may be implemented by a Conn.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if int64(len(args)) > math.MaxUint32 {
		return nil, driverError(c.log, fmt.Errorf("too many parameters (%d)", len(args)))
	} else if len(args) > math.MaxUint8 {
		protocol.EncodeExecSQLV1(&c.request, uint64(c.id), query, args)
	} else {
		protocol.EncodeExecSQLV0(&c.request, uint64(c.id), query, args)
	}

	var start time.Time
	if c.tracing != client.LogNone {
		start = time.Now()
	}
	err := c.protocol.Call(ctx, &c.request, &c.response)
	if c.tracing != client.LogNone {
		c.log(c.tracing, "%.3fs request exec: %q", time.Since(start).Seconds(), query)
	}
	if err != nil {
		return nil, driverError(c.log, err)
	}

	var result protocol.Result
	result, err = protocol.DecodeResult(&c.response)
	if err != nil {
		return nil, driverError(c.log, err)
	}

	return &Result{result: result}, nil
}

// Query is an optional interface that may be implemented by a Conn.
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, valuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if int64(len(args)) > math.MaxUint32 {
		return nil, driverError(c.log, fmt.Errorf("too many parameters (%d)", len(args)))
	} else if len(args) > math.MaxUint8 {
		protocol.EncodeQuerySQLV1(&c.request, uint64(c.id), query, args)
	} else {
		protocol.EncodeQuerySQLV0(&c.request, uint64(c.id), query, args)
	}

	var start time.Time
	if c.tracing != client.LogNone {
		start = time.Now()
	}
	err := c.protocol.Call(ctx, &c.request, &c.response)
	if c.tracing != client.LogNone {
		c.log(c.tracing, "%.3fs request query: %q", time.Since(start).Seconds(), query)
	}
	if err != nil {
		return nil, driverError(c.log, err)
	}

	var rows protocol.Rows
	rows, err = protocol.DecodeRows(&c.response)
	if err != nil {
		return nil, driverError(c.log, err)
	}

	return &Rows{
		ctx:      ctx,
		request:  &c.request,
		response: &c.response,
		protocol: c.protocol,
		rows:     rows,
		log:      c.log,
	}, nil
}

// Exec is an optional interface that may be implemented by a Conn.
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, valuesToNamedValues(args))
}

// Close invalidates and potentially stops any current prepared statements and
// transactions, marking this connection as no longer in use.
//
// Because the sql package maintains a free pool of connections and only calls
// Close when there's a surplus of idle connections, it shouldn't be necessary
// for drivers to do their own connection caching.
func (c *Conn) Close() error {
	return c.protocol.Close()
}

// BeginTx starts and returns a new transaction.  If the context is canceled by
// the user the sql package will call Tx.Rollback before discarding and closing
// the connection.
//
// This must check opts.Isolation to determine if there is a set isolation
// level. If the driver does not support a non-default level and one is set or
// if there is a non-default isolation level that is not supported, an error
// must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only value is
// true to either set the read-only transaction property if supported or return
// an error if it is not supported.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if _, err := c.ExecContext(ctx, "BEGIN", nil); err != nil {
		return nil, err
	}

	tx := &Tx{
		conn: c,
		log:  c.log,
	}

	return tx, nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (driver.Tx, error) {
	ctx := context.Background()

	if c.contextTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(context.Background(), c.contextTimeout)
		defer cancel()
	}

	return c.BeginTx(ctx, driver.TxOptions{})
}

// Tx is a transaction.
type Tx struct {
	conn *Conn
	log  client.LogFunc
}

// Commit the transaction.
func (tx *Tx) Commit() error {
	ctx := context.Background()

	if _, err := tx.conn.ExecContext(ctx, "COMMIT", nil); err != nil {
		return driverError(tx.log, err)
	}

	return nil
}

// Rollback the transaction.
func (tx *Tx) Rollback() error {
	ctx := context.Background()

	if _, err := tx.conn.ExecContext(ctx, "ROLLBACK", nil); err != nil {
		return driverError(tx.log, err)
	}

	return nil
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	protocol *protocol.Protocol
	request  *protocol.Message
	response *protocol.Message
	db       uint32
	id       uint32
	params   uint64
	log      client.LogFunc
	sql      string // Prepared SQL, only set when tracing
	tracing  client.LogLevel
}

// Close closes the statement.
func (s *Stmt) Close() error {
	protocol.EncodeFinalize(s.request, s.db, s.id)

	ctx := context.Background()

	if err := s.protocol.Call(ctx, s.request, s.response); err != nil {
		return driverError(s.log, err)
	}

	if err := protocol.DecodeEmpty(s.response); err != nil {
		return driverError(s.log, err)
	}

	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return int(s.params)
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if int64(len(args)) > math.MaxUint32 {
		return nil, driverError(s.log, fmt.Errorf("too many parameters (%d)", len(args)))
	} else if len(args) > math.MaxUint8 {
		protocol.EncodeExecV1(s.request, s.db, s.id, args)
	} else {
		protocol.EncodeExecV0(s.request, s.db, s.id, args)
	}

	var start time.Time
	if s.tracing != client.LogNone {
		start = time.Now()
	}
	err := s.protocol.Call(ctx, s.request, s.response)
	if s.tracing != client.LogNone {
		s.log(s.tracing, "%.3fs request prepared: %q", time.Since(start).Seconds(), s.sql)
	}
	if err != nil {
		return nil, driverError(s.log, err)
	}

	var result protocol.Result
	result, err = protocol.DecodeResult(s.response)
	if err != nil {
		return nil, driverError(s.log, err)
	}

	return &Result{result: result}, nil
}

// Exec executes a query that doesn't return rows, such
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if int64(len(args)) > math.MaxUint32 {
		return nil, driverError(s.log, fmt.Errorf("too many parameters (%d)", len(args)))
	} else if len(args) > math.MaxUint8 {
		protocol.EncodeQueryV1(s.request, s.db, s.id, args)
	} else {
		protocol.EncodeQueryV0(s.request, s.db, s.id, args)
	}

	var start time.Time
	if s.tracing != client.LogNone {
		start = time.Now()
	}
	err := s.protocol.Call(ctx, s.request, s.response)
	if s.tracing != client.LogNone {
		s.log(s.tracing, "%.3fs request prepared: %q", time.Since(start).Seconds(), s.sql)
	}
	if err != nil {
		return nil, driverError(s.log, err)
	}

	var rows protocol.Rows
	rows, err = protocol.DecodeRows(s.response)
	if err != nil {
		return nil, driverError(s.log, err)
	}

	return &Rows{ctx: ctx, request: s.request, response: s.response, protocol: s.protocol, rows: rows}, nil
}

// Query executes a query that may return rows, such as a
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// Result is the result of a query execution.
type Result struct {
	result protocol.Result
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	return int64(r.result.LastInsertID), nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	return int64(r.result.RowsAffected), nil
}

// Rows is an iterator over an executed query's results.
type Rows struct {
	ctx      context.Context
	protocol *protocol.Protocol
	request  *protocol.Message
	response *protocol.Message
	rows     protocol.Rows
	consumed bool
	types    []string
	log      client.LogFunc
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {
	return r.rows.Columns
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	err := r.rows.Close()

	// If we consumed the whole result set, there's nothing to do as
	// there's no pending response from the server.
	if r.consumed {
		return nil
	}

	// If there is was a single-response result set, we're done.
	if err == io.EOF {
		return nil
	}

	// Let's issue an interrupt request and wait until we get an empty
	// response, signalling that the query was interrupted.
	if err := r.protocol.Interrupt(r.ctx, r.request, r.response); err != nil {
		return driverError(r.log, err)
	}

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
func (r *Rows) Next(dest []driver.Value) error {
	err := r.rows.Next(dest)

	if err == protocol.ErrRowsPart {
		r.rows.Close()
		if err := r.protocol.More(r.ctx, r.response); err != nil {
			return driverError(r.log, err)
		}
		rows, err := protocol.DecodeRows(r.response)
		if err != nil {
			return driverError(r.log, err)
		}
		r.rows = rows
		return r.rows.Next(dest)
	}

	if err == io.EOF {
		r.consumed = true
	}

	return err
}

// ColumnTypeScanType implements RowsColumnTypeScanType.
func (r *Rows) ColumnTypeScanType(i int) reflect.Type {
	// column := sql.NewColumn(r.rows, i)

	// typ, err := r.protocol.ColumnTypeScanType(context.Background(), column)
	// if err != nil {
	// 	return nil
	// }

	// return typ.DriverType()
	return nil
}

// ColumnTypeDatabaseTypeName implements RowsColumnTypeDatabaseTypeName.
// warning: not thread safe
func (r *Rows) ColumnTypeDatabaseTypeName(i int) string {
	if r.types == nil {
		var err error
		r.types, err = r.rows.ColumnTypes()
		// an error might not matter if we get our types
		if err != nil && i >= len(r.types) {
			// a panic here doesn't really help,
			// as an empty column type is not the end of the world
			// but we should still inform the user of the failure
			const msg = "row (%p) error returning column #%d type: %v\n"
			r.log(client.LogWarn, msg, r, i, err)
			return ""
		}
	}
	return r.types[i]
}

// Convert a driver.Value slice into a driver.NamedValue slice.
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(args))
	for i, value := range args {
		namedValues[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}
	return namedValues
}

type unwrappable interface {
	Unwrap() error
}

// TODO driver.ErrBadConn should not be returned when there's a possibility that
// the query has been executed. In our case there is a window in protocol.Call
// between `send` and `recv` where the send has succeeded but the recv has
// failed. In those cases we call driverError on the result of protocol.Call,
// possibly returning ErrBadCon.
// https://cs.opensource.google/go/go/+/refs/tags/go1.20.4:src/database/sql/driver/driver.go;drc=a32a592c8c14927c20ac42808e1fb2e55b2e9470;l=162
func driverError(log client.LogFunc, err error) error {
	errno := syscall.Errno(0)
	if errors.As(err, &errno) && errno != 0 {
		log(client.LogDebug, "network connection lost: %v", errno.Error())
		return driver.ErrBadConn
	}

	netErr := &net.OpError{}
	if errors.As(err, &netErr) && netErr != nil {
		log(client.LogDebug, "network connection lost: %v", netErr.Error())
		return driver.ErrBadConn
	}

	pErr := protocol.ErrRequest{}
	if errors.As(err, &pErr) {
		switch pErr.Code {
		case errIoErrNotLeaderLegacy:
			fallthrough
		case errIoErrLeadershipLostLegacy:
			fallthrough
		case errIoErrNotLeader:
			fallthrough
		case errIoErrLeadershipLost:
			log(client.LogDebug, "leadership lost (%d - %s)", pErr.Code, pErr.Description)
			return driver.ErrBadConn
		case errNotFound:
			log(client.LogDebug, "not found - potentially after leadership loss (%d - %s)", pErr.Code, pErr.Description)
			return driver.ErrBadConn
		default:
			// FIXME: the server side sometimes return SQLITE_OK
			// even in case of errors. This issue is still being
			// investigated, but for now let's just mark this
			// connection as bad so the client will retry.
			if pErr.Code == 0 {
				log(client.LogWarn, "unexpected error code (%d - %s)", pErr.Code, pErr.Description)
				return driver.ErrBadConn
			}
			return Error{
				Code:    int(pErr.Code),
				Message: pErr.Description,
			}
		}
	}

	// When using a TLS connection, the underlying error might get
	// wrapped by the stdlib itself with the new errors wrapping
	// conventions available since go 1.13. In that case we check
	// the underlying error with Unwrap() instead of Cause().
	// Below error handling should be covered by net.OpError handling above.
	// if root, ok := err.(unwrappable); ok {
	// 	err = root.Unwrap()
	// }
	// switch err.(type) {
	// case *net.OpError:
	// 	log(client.LogDebug, "network connection lost: %v", err)
	// 	return driver.ErrBadConn
	// }

	if errors.Is(err, io.EOF) {
		log(client.LogDebug, "EOF detected: %v", err)
		return driver.ErrBadConn
	}

	return err
}
