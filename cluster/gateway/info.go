package gateway

import (
	"context"
	"log/slog"
	"os"

	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/internal/util/file"
)

// Load information about the cowsql node associated with this cluster member.
func loadInfo(database db.Node) (*db.RaftNode, error) {
	// Figure out if we actually need to act as cowsql node.
	var info *db.RaftNode

	err := transaction.Do(context.TODO(), database, func(ctx context.Context) error {
		tx := database

		var err error

		info, err = tx.DetermineRaftNode(ctx)

		return err
	})
	if err != nil {
		return nil, err
	}

	// If we're not part of the cowsql cluster, there's nothing to do.
	if info == nil {
		return nil, nil //nolint:nilnil
	}

	if info.Address == "" {
		// This is a standalone node not exposed to the network.
		info.Address = "1"
	}

	slog.Info("Starting database node", "id", info.ID, "local", info.Address, "role", info.Role)

	// Data directory
	dir := database.GlobalDatabaseDir()
	if !file.PathExists(dir) {
		err := os.Mkdir(dir, 0o750)
		if err != nil {
			return nil, err
		}
	}

	return info, nil
}
