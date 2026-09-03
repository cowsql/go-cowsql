package membership

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
)

// MaybeUpdate Check this node's version and possibly run the executable returned by PreUpdateCheck.
func MaybeUpdate(g cluster.Gateway) error {
	shouldUpdate := false

	enabled, err := Enabled(g.Node())
	if err != nil {
		return fmt.Errorf("Failed to check clustering is enabled: %w", err)
	}

	if !enabled {
		return nil
	}

	if g.Cluster() == nil {
		return errors.New("Failed checking cluster update, state not initialized yet")
	}

	err = transaction.Do(context.TODO(), g.Cluster(), func(ctx context.Context) error {
		tx := g.Cluster()
		outdated, err := tx.NodeIsOutdated(ctx)
		if err != nil {
			return err
		}

		shouldUpdate = outdated
		return nil
	})
	if err != nil {
		// Just log the error and return.
		return fmt.Errorf("Failed to check if this node is out-of-date: %w", err)
	}

	if !shouldUpdate {
		slog.Debug("Cluster node is up-to-date")
		return nil
	}

	return TriggerUpdate(g)
}

func TriggerUpdate(g cluster.Gateway) error {
	slog.Warn("Member is out-of-date with respect to other cluster members")

	updateFunc, err := g.UserConfig().PreUpdateCheckFunc()()
	if err != nil {
		return err
	}

	if updateFunc == nil {
		slog.Debug("No update check enabled, skipping auto-update")
		return nil
	}

	return updateFunc()
}

// UpgradeMembersWithoutRole assigns the Spare raft role to all cluster members that are not currently part of the
// raft configuration. It's used for upgrading a cluster from a version without roles support.
func UpgradeMembersWithoutRole(gateway cluster.Gateway, members []db.NodeInfo) error {
	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	if errors.Is(err, ErrNotLeader) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("Failed to get current raft members: %w", err)
	}

	// Convert raft node list to map keyed on ID.
	raftNodeIDs := map[uint64]bool{}
	for _, node := range nodes {
		raftNodeIDs[node.ID] = true
	}

	cowsqlClient, err := gateway.RaftClient(context.TODO())
	if err != nil {
		return fmt.Errorf("Failed to connect to local cowsql member: %w", err)
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client", "err", err)
		}
	}()

	// Check that each member is present in the raft configuration, and add it if not.
	for _, member := range members {
		found := false
		for _, node := range nodes {
			if member.ID == 1 && node.ID == 1 || member.Address == node.Address {
				found = true
				break
			}
		}
		if found {
			continue
		}

		// Try to use the same ID as the node, but it might not be possible if it's use.
		id := uint64(member.ID)
		_, ok := raftNodeIDs[id]
		if ok {
			for _, other := range members {
				_, ok := raftNodeIDs[uint64(other.ID)]
				if !ok {
					id = uint64(other.ID) // Found unused raft ID for member.
					break
				}
			}

			// This can't really happen (but has in the past) since there are always at least as many
			// members as there are nodes, and all of them have different IDs.
			if id == uint64(member.ID) {
				slog.Error("No available raft ID for cluster member", "memberID", member.ID, "members", members, "raftMembers", nodes)
				return fmt.Errorf("No available raft ID for cluster member ID %d", member.ID)
			}
		}
		raftNodeIDs[id] = true

		info := db.RaftNode{
			NodeInfo: client.NodeInfo{
				ID:      id,
				Address: member.Address,
				Role:    db.RaftSpare,
			},
		}

		slog.Info("Add spare cowsql node", "id", info.ID, "address", info.Address)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = cowsqlClient.Add(ctx, info.NodeInfo)
		cancel()
		if err != nil {
			return fmt.Errorf("Failed to add cowsql member: %w", err)
		}
	}

	return nil
}
