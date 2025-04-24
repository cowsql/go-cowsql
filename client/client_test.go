package client_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	cowsql "github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/internal/protocol"
)

var (
	assertLen     = requireLen
	assertNoError = requireNoError
)

func assertTrue(t *testing.T, ok bool) {
	t.Helper()
	if !ok {
		t.Fatal(ok)
	}
}

func requireLen(t *testing.T, x any, l int) {
	t.Helper()
	v := reflect.ValueOf(x)
	if l != v.Len() {
		t.Fatal()
	}
}

func assertEqual(t *testing.T, expected, actual interface{}) {
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

func TestClient_Leader(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := client.New(ctx, node.BindAddress())
	requireNoError(t, err)
	defer client.Close()

	leader, err := client.Leader(context.Background())
	requireNoError(t, err)

	assertEqual(t, leader.ID, uint64(1))
	assertEqual(t, leader.Address, "@1001")
}

func TestClient_Dump(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := client.New(ctx, node.BindAddress())
	requireNoError(t, err)
	defer client.Close()

	// Open a database and create a test table.
	request := protocol.Message{}
	request.Init(4096)

	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeOpen(&request, "test.db", 0, "volatile")

	p := client.Protocol()
	err = p.Call(ctx, &request, &response)
	requireNoError(t, err)

	db, err := protocol.DecodeDb(&response)
	requireNoError(t, err)

	protocol.EncodeExecSQLV0(&request, uint64(db), "CREATE TABLE foo (n INT)", nil)

	err = p.Call(ctx, &request, &response)
	requireNoError(t, err)

	files, err := client.Dump(ctx, "test.db")
	requireNoError(t, err)

	requireLen(t, files, 2)
	assertEqual(t, "test.db", files[0].Name)
	assertEqual(t, 4096, len(files[0].Data))

	assertEqual(t, "test.db-wal", files[1].Name)
	assertEqual(t, 8272, len(files[1].Data))
}

func TestClient_Cluster(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := client.New(ctx, node.BindAddress())
	requireNoError(t, err)
	defer cli.Close()

	servers, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertLen(t, servers, 1)
	assertEqual(t, servers[0].ID, uint64(1))
	assertEqual(t, servers[0].Address, "@1001")
	assertEqual(t, servers[0].Role, client.Voter)
}

func TestClient_Transfer(t *testing.T) {
	node1, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cli, err := client.New(ctx, node1.BindAddress())
	requireNoError(t, err)
	defer cli.Close()

	node2, cleanup := addNode(t, cli, 2)
	defer cleanup()

	err = cli.Assign(context.Background(), 2, client.Voter)
	requireNoError(t, err)

	err = cli.Transfer(context.Background(), 2)
	requireNoError(t, err)

	leader, err := cli.Leader(context.Background())
	requireNoError(t, err)
	assertEqual(t, leader.ID, uint64(2))

	cli, err = client.New(ctx, node2.BindAddress())
	requireNoError(t, err)
	defer cli.Close()

	leader, err = cli.Leader(context.Background())
	requireNoError(t, err)
	assertEqual(t, leader.ID, uint64(2))
}

func TestClient_Describe(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := client.New(ctx, node.BindAddress())
	requireNoError(t, err)
	defer cli.Close()

	metadata, err := cli.Describe(context.Background())
	requireNoError(t, err)

	assertEqual(t, uint64(0), metadata.FailureDomain)
	assertEqual(t, uint64(0), metadata.Weight)

	requireNoError(t, cli.Weight(context.Background(), 123))

	metadata, err = cli.Describe(context.Background())
	requireNoError(t, err)

	assertEqual(t, uint64(0), metadata.FailureDomain)
	assertEqual(t, uint64(123), metadata.Weight)
}

func newNode(t *testing.T) (*cowsql.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	id := uint64(1)
	address := fmt.Sprintf("@%d", id+1000)
	node, err := cowsql.New(uint64(1), address, dir, cowsql.WithBindAddress(address))
	requireNoError(t, err)

	err = node.Start()
	requireNoError(t, err)

	cleanup := func() {
		requireNoError(t, node.Close())
		dirCleanup()
	}

	return node, cleanup
}

func addNode(t *testing.T, cli *client.Client, id uint64) (*cowsql.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	address := fmt.Sprintf("@%d", id+1000)
	node, err := cowsql.New(id, address, dir, cowsql.WithBindAddress(address))
	requireNoError(t, err)

	err = node.Start()
	requireNoError(t, err)

	info := client.NodeInfo{
		ID:      id,
		Address: address,
		Role:    client.Spare,
	}

	err = cli.Add(ctx, info)
	requireNoError(t, err)

	cleanup := func() {
		requireNoError(t, node.Close())
		dirCleanup()
	}

	return node, cleanup
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
