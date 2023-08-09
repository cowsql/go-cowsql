package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cowsql "github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	"github.com/stretchr/testify/require"
)

func TestMembership(t *testing.T) {
	n := 3
	nodes := make([]*cowsql.Node, n)
	infos := make([]client.NodeInfo, n)

	for i := range nodes {
		id := uint64(i + 1)
		address := fmt.Sprintf("@test-%d", id)
		dir, cleanup := newDir(t)
		defer cleanup()
		node, err := cowsql.New(id, address, dir, cowsql.WithBindAddress(address))
		require.NoError(t, err)
		nodes[i] = node
		infos[i].ID = id
		infos[i].Address = address
		err = node.Start()
		require.NoError(t, err)
		defer node.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	store := client.NewInmemNodeStore()
	store.Set(context.Background(), []client.NodeInfo{infos[0]})

	client, err := client.FindLeader(ctx, store)
	require.NoError(t, err)
	defer client.Close()

	err = client.Add(ctx, infos[1])
	require.NoError(t, err)
}
