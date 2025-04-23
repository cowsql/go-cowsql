package protocol_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cowsql/go-cowsql/internal/protocol"
	"github.com/cowsql/go-cowsql/logging"
)

func requireNoError(t *testing.T, err error) {
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

// func TestProtocol_Heartbeat(t *testing.T) {
// 	c, cleanup := newProtocol(t)
// 	defer cleanup()

// 	request, response := newMessagePair(512, 512)

// 	protocol.EncodeHeartbeat(&request, uint64(time.Now().Unix()))

// 	makeCall(t, c, &request, &response)

// 	servers, err := protocol.DecodeNodes(&response)
// 	requireNoError(t, err)

// 	assert.Len(t, servers, 2)
// 	assertEqual(t, client.Nodes{
// 		{ID: uint64(1), Address: "1.2.3.4:666"},
// 		{ID: uint64(2), Address: "5.6.7.8:666"}},
// 		servers)
// }

// Test sending a request that needs to be written into the dynamic buffer.
func TestProtocol_RequestWithDynamicBuffer(t *testing.T) {
	p, cleanup := newProtocol(t)
	defer cleanup()

	request, response := newMessagePair(64, 64)

	protocol.EncodeOpen(&request, "test.db", 0, "test-0")

	makeCall(t, p, &request, &response)

	id, err := protocol.DecodeDb(&response)
	requireNoError(t, err)

	sql := `
CREATE TABLE foo (n INT);
CREATE TABLE bar (n INT);
CREATE TABLE egg (n INT);
CREATE TABLE baz (n INT);
`
	protocol.EncodeExecSQLV0(&request, uint64(id), sql, nil)

	makeCall(t, p, &request, &response)
}

func TestProtocol_Prepare(t *testing.T) {
	c, cleanup := newProtocol(t)
	defer cleanup()

	request, response := newMessagePair(64, 64)

	protocol.EncodeOpen(&request, "test.db", 0, "test-0")

	makeCall(t, c, &request, &response)

	db, err := protocol.DecodeDb(&response)
	requireNoError(t, err)

	protocol.EncodePrepare(&request, uint64(db), "CREATE TABLE test (n INT)")

	makeCall(t, c, &request, &response)

	_, stmt, params, err := protocol.DecodeStmt(&response)
	requireNoError(t, err)

	assertEqual(t, uint32(0), stmt)
	assertEqual(t, uint64(0), params)
}

/*
func TestProtocol_Exec(t *testing.T) {
	client, cleanup := newProtocol(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := client.Open(ctx, "test.db", "volatile")
	requireNoError(t, err)

	stmt, err := client.Prepare(ctx, db.ID, "CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	requireNoError(t, err)
}

func TestProtocol_Query(t *testing.T) {
	client, cleanup := newProtocol(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := client.Open(ctx, "test.db", "volatile")
	requireNoError(t, err)

	start := time.Now()

	stmt, err := client.Prepare(ctx, db.ID, "CREATE TABLE test (n INT)")
	requireNoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	requireNoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	requireNoError(t, err)

	stmt, err = client.Prepare(ctx, db.ID, "INSERT INTO test VALUES(1)")
	requireNoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	requireNoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	requireNoError(t, err)

	stmt, err = client.Prepare(ctx, db.ID, "SELECT n FROM test")
	requireNoError(t, err)

	_, err = client.Query(ctx, db.ID, stmt.ID)
	requireNoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	requireNoError(t, err)

	fmt.Printf("time %s\n", time.Since(start))
}
*/

func newProtocol(t *testing.T) (*protocol.Protocol, func()) {
	t.Helper()

	address, serverCleanup := newNode(t, 0)

	store := newStore(t, []string{address})
	config := protocol.Config{
		AttemptTimeout: 100 * time.Millisecond,
		BackoffFactor:  time.Millisecond,
	}
	connector := protocol.NewConnector(0, store, config, logging.Test(t))

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)

	requireNoError(t, err)

	cleanup := func() {
		client.Close()
		serverCleanup()
	}

	return client, cleanup
}

// Perform a client call.
func makeCall(t *testing.T, p *protocol.Protocol, request, response *protocol.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := p.Call(ctx, request, response)
	requireNoError(t, err)
}

// Return a new message pair to be used as request and response.
func newMessagePair(size1, size2 int) (protocol.Message, protocol.Message) {
	message1 := protocol.Message{}
	message1.Init(size1)

	message2 := protocol.Message{}
	message2.Init(size2)

	return message1, message2
}
