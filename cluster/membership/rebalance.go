package membership

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
)

type ChangeMemberRoleFunc func(ctx context.Context, address string, nodes []db.RaftNode) error

func RebalanceMembersHook(ctx context.Context, gateway cluster.Gateway, localClusterAddress string, heartbeatData *heartbeat.APIHeartbeat, isLeader bool, unavailableMembers []string, alwaysDemoteRoles []string, changeMemberRoleFunc ChangeMemberRoleFunc) {
	// If we are leader and called from the leader heartbeat send function (unavailbleMembers != nil) and there
	// are other members in the cluster, then check if we need to update roles. We do not want to do this if
	// we are called on the leader as part of a notification heartbeat being received from another member.
	if isLeader && unavailableMembers != nil && len(heartbeatData.Members) > 1 {
		isDegraded := false
		hasNodesNotPartOfRaft := false
		hasMustDemoteRoles := false
		onlineVoters := 0
		onlineStandbys := 0

		for _, member := range heartbeatData.Members {
			role := db.RaftRole(member.RaftRole)
			if member.Online {
				// Count online members that have voter or stand-by raft role.
				switch role {
				case db.RaftVoter:
					onlineVoters++
				case db.RaftStandBy:
					onlineStandbys++
				}

				if member.RaftID == 0 {
					hasNodesNotPartOfRaft = true
				}

				// Check if a 'database-client' node currently has a raft role other than 'spare'.
				isMustDemote := slices.ContainsFunc(member.Roles, func(role string) bool { return slices.Contains(alwaysDemoteRoles, role) })
				if isMustDemote && member.RaftRole != int(db.RaftSpare) {
					hasMustDemoteRoles = true
				}
			} else if role != db.RaftSpare {
				isDegraded = true // Offline member that has voter or stand-by raft role.
			}
		}

		maxVoters := gateway.UserConfig().MaxVotersFunc()()
		maxStandBy := gateway.UserConfig().MaxStandbyFunc()()

		// If there are offline members that have voter or stand-by database roles, let's see if we can
		// replace them with spare ones. Also, if we don't have enough voters or standbys, let's see if we
		// can upgrade some member.
		if isDegraded || onlineVoters != int(maxVoters) || onlineStandbys != int(maxStandBy) || hasMustDemoteRoles {
			slog.Debug("Rebalancing member roles in heartbeat", slog.String("local_address", localClusterAddress))
			err := rebalanceMemberRoles(ctx, gateway, unavailableMembers, alwaysDemoteRoles, changeMemberRoleFunc)
			if err != nil && !errors.Is(err, ErrNotLeader) {
				slog.Warn("Could not rebalance cluster member roles", slog.Any("err", err), slog.String("local_address", localClusterAddress))
			}

		}

		if hasNodesNotPartOfRaft {
			slog.Debug("Upgrading members without raft role in heartbeat", slog.String("local_address", localClusterAddress))
			err := upgradeNodesWithoutRaftRole(ctx, gateway, gateway.Cluster())
			if err != nil && !errors.Is(err, ErrNotLeader) {
				slog.Warn("Failed upgrading raft roles:", slog.Any("err", err), slog.String("local_address", localClusterAddress))
			}
		}
	}
}

// Check if there's a cowsql node whose role should be changed, and post a
// change role request if so.
func rebalanceMemberRoles(ctx context.Context, gateway cluster.Gateway, unavailableMembers []string, exceptRoles []string, changeMemberRoleFunc ChangeMemberRoleFunc) error {
	if ctx.Err() != nil {
		return nil
	}

again:
	address, nodes, err := Rebalance(gateway, unavailableMembers, exceptRoles)
	if err != nil {
		return err
	}

	if address == "" {
		// Nothing to do.
		return nil
	}

	// Process demotions of offline nodes immediately.
	for _, member := range nodes {
		if member.Address != address {
			continue
		}

		reachable := cluster.HasConnectivity(gateway.NetworkCert(), gateway.ServerCert(), address, gateway.UserConfig().RestrictTLS())

		log := slog.With(slog.String("name", member.Name), slog.Int("role", int(member.Role)))
		if member.Role != db.RaftSpare {
			if !reachable {
				// The server isn't ready to be promoted yet, try again next time.
				return nil
			}

			log.Info("Promoting cluster member")
			break
		}

		if reachable {
			// Don't demote reachable servers.
			break
		}

		log.Info("Demoting cluster member")

		err := gateway.DemoteOfflineNode(member.ID)
		if err != nil {
			return fmt.Errorf("Failed to demote cluster member %q: %w", member.Name, err)
		}

		goto again
	}

	// Then handle the promotions.
	err = changeMemberRoleFunc(ctx, address, nodes)
	if err != nil {
		return err
	}

	goto again
}

func upgradeNodesWithoutRaftRole(ctx context.Context, gateway cluster.Gateway, globalDB db.Cluster) error {
	var members []db.NodeInfo
	err := transaction.Do(ctx, globalDB, func(ctx context.Context) error {
		var err error
		members, err = globalDB.GetNodes(ctx)
		if err != nil {
			return fmt.Errorf("Failed getting cluster members: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return UpgradeMembersWithoutRole(gateway, members)
}
