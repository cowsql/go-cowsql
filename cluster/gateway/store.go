package gateway

import (
	"context"

	"github.com/cowsql/go-cowsql/client"
)

// Conditionally uses the in-memory or the on-disk server store.
type cowsqlNodeStore struct {
	inMemory client.NodeStore
	onDisk   client.NodeStore
}

// Get returns the list of servers from the active node store.
func (s *cowsqlNodeStore) Get(ctx context.Context) ([]client.NodeInfo, error) {
	if s.inMemory != nil {
		return s.inMemory.Get(ctx)
	}

	return s.onDisk.Get(ctx)
}

// Set stores the list of servers in the active node store.
func (s *cowsqlNodeStore) Set(ctx context.Context, servers []client.NodeInfo) error {
	if s.inMemory != nil {
		return s.inMemory.Set(ctx, servers)
	}

	return s.onDisk.Set(ctx, servers)
}
