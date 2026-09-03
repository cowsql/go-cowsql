package membership

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/logging"
)

// ListDatabaseNodes returns a list of database node names.
func ListDatabaseNodes(database db.Node) ([]string, error) {
	nodes := []db.RaftNode{}
	err := transaction.Do(context.TODO(), database, func(ctx context.Context) error {
		tx := database
		var err error
		nodes, err = tx.GetRaftNodes(ctx)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to list database nodes: %w", err)
	}

	addresses := make([]string, 0)
	for _, raftNode := range nodes {
		if raftNode.Role != db.RaftVoter {
			continue
		}

		addresses = append(addresses, raftNode.Address)
	}

	return addresses, nil
}

// Recover attempts data recovery on the cluster database.
func Recover(database db.Node) error {
	// Figure out if we actually act as cowsql node.
	var info *db.RaftNode
	err := transaction.Do(context.TODO(), database, func(ctx context.Context) error {
		tx := database
		var err error
		info, err = tx.DetermineRaftNode(ctx)
		return err
	})
	if err != nil {
		return fmt.Errorf("Failed to determine node role: %w", err)
	}

	// If we're not a database node, return an error.
	if info == nil {
		return errors.New("This server has no database role")
	}

	// If this is a standalone node not exposed to the network, return an
	// error.
	if info.Address == "" {
		return errors.New("This server is not clustered")
	}

	dir := database.GlobalDatabaseDir()
	server, err := cowsql.New(
		uint64(info.ID),
		info.Address,
		dir,
	)
	if err != nil {
		return fmt.Errorf("Failed to create cowsql server: %w", err)
	}

	cluster := []cowsql.NodeInfo{
		{ID: uint64(info.ID), Address: info.Address},
	}

	err = server.Recover(cluster)
	if err != nil {
		return fmt.Errorf("Failed to recover database state: %w", err)
	}

	// Update the list of raft nodes.
	err = transaction.Do(context.TODO(), database, func(ctx context.Context) error {
		tx := database
		nodes := []db.RaftNode{
			{
				NodeInfo: client.NodeInfo{
					ID:      info.ID,
					Address: info.Address,
				},
				Name: info.Name,
			},
		}

		return tx.ReplaceRaftNodes(ctx, nodes)
	})
	if err != nil {
		return fmt.Errorf("Failed to update database nodes: %w", err)
	}

	return nil
}

// Reconfigure replaces the entire cluster configuration.
// Addresses and node roles may be updated. Node IDs are read-only.
func Reconfigure(database db.Node, raftNodes []db.RaftNode, patchFunc func(database db.Node, nodes []client.NodeInfo) error) error {
	var info *db.RaftNode
	err := transaction.Do(context.TODO(), database, func(ctx context.Context) error {
		tx := database
		var err error
		info, err = tx.DetermineRaftNode(ctx)

		return err
	})
	if err != nil {
		return fmt.Errorf("Failed to determine cluster member raft role: %w", err)
	}

	if info == nil {
		return errors.New("This cluster member has no raft role")
	}

	localAddress := info.Address

	nodes := make([]client.NodeInfo, 0, len(raftNodes))
	for _, raftNode := range raftNodes {
		nodes = append(nodes, raftNode.NodeInfo)

		// Get the new address for this node.
		if raftNode.ID == info.ID {
			localAddress = raftNode.Address
		}
	}

	// Update cluster.https_address if changed.
	if localAddress != info.Address {
		err := transaction.Do(context.TODO(), database, func(ctx context.Context) error {
			tx := database
			return tx.SetClusterAddress(ctx, localAddress)
		})
		if err != nil {
			return fmt.Errorf("Failed to update node configuration: %w", err)
		}
	}

	dir := database.GlobalDatabaseDir()
	// Replace cluster configuration in cowsql.
	err = cowsql.ReconfigureMembershipExt(dir, nodes)
	if err != nil {
		return fmt.Errorf("Failed to recover database state: %w", err)
	}

	// Replace cluster configuration in local raft_nodes database.
	err = transaction.Do(context.TODO(), database, func(ctx context.Context) error {
		tx := database
		return tx.ReplaceRaftNodes(ctx, raftNodes)
	})
	if err != nil {
		return err
	}

	// Create patch file for global nodes database.
	return patchFunc(database, nodes)
}

// RemoveRaftNode removes a raft node from the raft configuration.
func RemoveRaftNode(gateway cluster.Gateway, address string) error {
	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	if err != nil {
		return fmt.Errorf("Failed to get current raft nodes: %w", err)
	}

	var id uint64
	for _, raftNode := range nodes {
		if raftNode.Address == address {
			id = raftNode.ID
			break
		}
	}
	if id == 0 {
		return fmt.Errorf("No raft node with address %q", address)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cowsqlClient, err := client.FindLeader(
		ctx, gateway.NodeStore(),
		client.WithDialFunc(gateway.RaftDial()),
		client.WithLogFunc(logging.CowsqlLog),
	)
	if err != nil {
		return fmt.Errorf("Failed to connect to cluster leader: %w", err)
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client", "err", err)
		}
	}()

	err = cowsqlClient.Remove(ctx, id)
	if err != nil {
		return fmt.Errorf("Failed to remove node: %w", err)
	}

	return nil
}
