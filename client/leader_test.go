package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cowsql "github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
)

func requireNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func TestMembership(t *testing.T) {
	n := 3
	nodes := make([]*cowsql.Node, n)
	infos := make([]client.NodeInfo, n)

	for i := range nodes {
		id := uint64(i + 1)
		address := fmt.Sprintf("@test-%d", id)

		dir, cleanup := newDir(t)
		defer cleanup() //nolint:revive

		node, err := cowsql.New(id, address, dir, cowsql.WithBindAddress(address))
		requireNoError(t, err)

		nodes[i] = node
		infos[i].ID = id
		infos[i].Address = address
		err = node.Start()
		requireNoError(t, err)

		defer node.Close() //nolint:revive
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	store := client.NewInmemNodeStore()
	requireNoError(t, store.Set(context.Background(), []client.NodeInfo{infos[0]}))

	c, err := client.FindLeader(ctx, store)
	requireNoError(t, err)

	defer c.Close()

	err = c.Add(ctx, infos[1])
	requireNoError(t, err)
}
