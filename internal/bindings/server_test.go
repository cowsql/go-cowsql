package bindings_test

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cowsql/go-cowsql/internal/bindings"
	"github.com/cowsql/go-cowsql/internal/protocol"
)

var (
	assertEqual   = requireEqual
	assertNoError = requireNoError
)

func assertTrue(t *testing.T, ok bool) {
	t.Helper()
	if !ok {
		t.Fatal(ok)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireEqual(t *testing.T, expected, actual interface{}) {
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

func TestNode_Create(t *testing.T) {
	_, cleanup := newNode(t)
	defer cleanup()
}

func TestNode_Start(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	requireNoError(t, err)
	defer server.Close()

	err = server.SetBindAddress("@")
	requireNoError(t, err)

	err = server.Start()
	requireNoError(t, err)

	conn, err := net.Dial("unix", server.GetBindAddress())
	requireNoError(t, err)
	conn.Close()

	assertTrue(t, strings.HasPrefix(server.GetBindAddress(), "@"))

	err = server.Stop()
	requireNoError(t, err)
}

func TestNode_Restart(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	requireNoError(t, err)

	requireNoError(t, server.SetBindAddress("@abc"))
	requireNoError(t, server.Start())

	requireNoError(t, server.Stop())
	server.Close()

	server, err = bindings.NewNode(context.Background(), 1, "1", dir)
	requireNoError(t, err)

	requireNoError(t, server.SetBindAddress("@abc"))
	requireNoError(t, server.Start())

	requireNoError(t, server.Stop())
	server.Close()
}

func TestNode_Start_Inet(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	requireNoError(t, err)
	defer server.Close()

	err = server.SetBindAddress("127.0.0.1:9000")
	requireNoError(t, err)

	err = server.Start()
	requireNoError(t, err)

	conn, err := net.Dial("tcp", server.GetBindAddress())
	requireNoError(t, err)
	conn.Close()

	err = server.Stop()
	requireNoError(t, err)
}

func TestNode_Leader(t *testing.T) {
	_, cleanup := newNode(t)
	defer cleanup()

	conn := newClient(t)

	// Make a Leader request
	buf := makeClientRequest(t, conn, protocol.RequestLeader)
	assertEqual(t, uint8(1), buf[0])

	requireNoError(t, conn.Close())
}

// func TestNode_Heartbeat(t *testing.T) {
// 	server, cleanup := newNode(t)
// 	defer cleanup()

// 	listener, cleanup := newListener(t)
// 	defer cleanup()

// 	cleanup = runNode(t, server, listener)
// 	defer cleanup()

// 	conn := newClient(t, listener)

// 	// Make a Heartbeat request
// 	makeClientRequest(t, conn, bindings.RequestHeartbeat)

// 	requireNoError(t, conn.Close())
// }

// func TestNode_ConcurrentHandleAndClose(t *testing.T) {
// 	server, cleanup := newNode(t)
// 	defer cleanup()

// 	listener, cleanup := newListener(t)
// 	defer cleanup()

// 	acceptCh := make(chan error)
// 	go func() {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			acceptCh <- err
// 		}
// 		server.Handle(conn)
// 		acceptCh <- nil
// 	}()

// 	conn, err := net.Dial("unix", listener.Addr().String())
// 	requireNoError(t, err)

// 	requireNoError(t, conn.Close())

// 	assertNoError(t, <-acceptCh)
// }

// Create a new Node object for tests.
func newNode(t *testing.T) (*bindings.Node, func()) {
	t.Helper()

	dir, dirCleanup := newDir(t)

	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	requireNoError(t, err)

	err = server.SetBindAddress("@test")
	requireNoError(t, err)

	requireNoError(t, server.Start())

	cleanup := func() {
		requireNoError(t, server.Stop())
		server.Close()
		dirCleanup()
	}

	return server, cleanup
}

// Create a new client network connection, performing the handshake.
func newClient(t *testing.T) net.Conn {
	t.Helper()

	conn, err := net.Dial("unix", "@test")
	requireNoError(t, err)

	// Handshake
	err = binary.Write(conn, binary.LittleEndian, protocol.VersionLegacy)
	requireNoError(t, err)

	return conn
}

// Perform a client request.
func makeClientRequest(t *testing.T, conn net.Conn, kind byte) []byte {
	t.Helper()

	// Number of words
	err := binary.Write(conn, binary.LittleEndian, uint32(1))
	requireNoError(t, err)

	// Type, flags, extra.
	n, err := conn.Write([]byte{kind, 0, 0, 0})
	requireNoError(t, err)
	requireEqual(t, 4, n)

	n, err = conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Unused single-word request payload
	requireNoError(t, err)
	requireEqual(t, 8, n)

	// Read the response
	conn.SetDeadline(time.Now().Add(250 * time.Millisecond))
	buf := make([]byte, 64)
	_, err = conn.Read(buf)
	requireNoError(t, err)

	return buf
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
