package membership

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/cowsql/go-cowsql/app"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/logging"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
)

// Bootstrap turns a non-clustered server into the first (and leader)
// member of a new cluster.
//
// This instance must already have its cluster.https_address set and be listening
// on the associated network address.
func Bootstrap(gateway cluster.Gateway, serverName string) error {
	s := gateway.State()
	// Check parameters
	if serverName == "" {
		return errors.New("Server name must not be empty")
	}

	var localClusterAddress string

	err := transaction.Do(context.TODO(), gateway.Node(), func(ctx context.Context) error {
		tx := gateway.Node()

		var err error

		localClusterAddress, err = tx.GetClusterAddress(ctx)
		if err != nil {
			return fmt.Errorf("Failed to fetch cluster address configuration: %w", err)
		}

		// Make sure node-local database state is in order.
		err = membershipCheckNodeStateForBootstrapOrJoin(ctx, tx, localClusterAddress)
		if err != nil {
			return err
		}

		err = tx.CreateFirstRaftNode(ctx, localClusterAddress, serverName)
		if err != nil {
			return fmt.Errorf("Failed to insert first raft node: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Update our own entry in the nodes table.
	err = transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()
		// Make sure cluster database state is in order.
		err := membershipCheckClusterStateForBootstrapOrJoin(ctx, tx)
		if err != nil {
			return err
		}

		// Add ourselves to the nodes table.
		err = tx.BootstrapNode(ctx, serverName, localClusterAddress)
		if err != nil {
			return fmt.Errorf("Failed updating cluster member: %w", err)
		}

		serverCertx509, err := gateway.ServerCert().PublicKeyX509()
		if err != nil {
			return err
		}

		err = tx.SetNodeCertificateByName(ctx, serverName, serverCertx509)
		if err != nil {
			return fmt.Errorf("Failed ensuring server certificate is trusted: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Reload the trusted certificate cache to enable the certificate we just added to the local trust store
	// to be used when validating endpoint connections. This will allow Cowsql to connect to ourselves.
	err = s.UpdateAuthorizer(context.TODO())
	if err != nil {
		return err
	}

	// Shutdown the gateway. This will trash any cowsql connection against
	// our in-memory cowsql driver and shutdown the associated raft
	// instance. We also lock regular access to the cluster database since
	// we don't want any other database code to run while we're
	// reconfiguring raft.
	err = gateway.Cluster().EnterExclusive()
	if err != nil {
		return fmt.Errorf("Failed to acquire cluster database lock: %w", err)
	}

	err = gateway.ShutdownServer()
	if err != nil {
		return fmt.Errorf("Failed to shutdown gRPC SQL gateway: %w", err)
	}

	// If endpoint listeners are active, apply new cluster certificate.

	if s.HasListener(context.TODO()) == nil {
		// Generate a new cluster certificate.
		clusterCert, err := gateway.State().NewClusterCertificate()
		if err != nil {
			return fmt.Errorf("Failed to create cluster cert: %w", err)
		}

		gateway.NetworkUpdateCert(clusterCert)
	}

	// Re-initialize the gateway. This will create a new raft factory an
	// cowsql driver instance, which will be exposed over gRPC by the
	// gateway handlers.
	err = gateway.Initialize(true)
	if err != nil {
		return fmt.Errorf("Failed to re-initialize gRPC SQL gateway: %w", err)
	}

	err = gateway.WaitLeadership()
	if err != nil {
		return err
	}

	// Make sure we can actually connect to the cluster database through
	// the network endpoint. This also releases the previously acquired
	// lock and makes the Go SQL pooling system invalidate the old
	// connection, so new queries will be executed over the new network
	// connection.
	err = transaction.DoExclusive(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()

		_, err = tx.GetNodes(ctx)
		if err != nil {
			return fmt.Errorf("Failed getting cluster members: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("(Bootstrap) Cluster database initialization failed: %w", err)
	}

	return nil
}

// Accept a new node and add it to the cluster.
//
// This instance must already be clustered.
//
// Return an updated list raft database nodes (possibly including the newly
// accepted node).
func Accept(gateway cluster.Gateway, serverCert *x509.Certificate, name, address string, schema, api, arch int) ([]db.RaftNode, error) {
	// Check parameters
	if name == "" {
		return nil, errors.New("Member name must not be empty")
	}

	if address == "" {
		return nil, errors.New("Member address must not be empty")
	}

	// Insert the new node into the nodes table.
	var id int64

	err := transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()
		// Check that the node can be accepted with these parameters.
		err := membershipCheckClusterStateForAccept(ctx, gateway.Options().Version(), tx, name, address, schema, api)
		if err != nil {
			return err
		}

		// Add the new node.
		id, err = tx.CreateNode(ctx, name, address, arch)
		if err != nil {
			return fmt.Errorf("Failed to insert new node into the database: %w", err)
		}

		// Mark the node as pending, so it will be skipped when
		// performing heartbeats or sending cluster
		// notifications.
		err = tx.SetNodePendingFlag(ctx, id, true)
		if err != nil {
			return fmt.Errorf("Failed to mark the new node as pending: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Possibly insert the new node into the raft_nodes table (if we have
	// less than 3 database nodes).
	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("Failed to get raft nodes from the log: %w", err)
	}

	count := len(nodes) // Existing nodes
	voters := 0
	standbys := 0

	for _, raftNode := range nodes {
		switch raftNode.Role {
		case db.RaftVoter:
			voters++
		case db.RaftStandBy:
			standbys++
		}
	}

	raftNode := db.RaftNode{
		ID:      uint64(id), //nolint:gosec
		Address: address,
		Role:    db.RaftSpare,
		Name:    name,
	}

	if count > 1 && voters < int(gateway.Options().MaxVotersFunc()()) {
		raftNode.Role = db.RaftVoter
	} else if standbys < int(gateway.Options().MaxStandbyFunc()()) {
		raftNode.Role = db.RaftStandBy
	}

	nodes = append(nodes, raftNode)

	err = transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()

		err = tx.SetNodeCertificateByName(ctx, name, serverCert)
		if err != nil {
			return fmt.Errorf("Failed ensuring server certificate is trusted: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Reload the trusted certificate cache to enable the certificate we just added to the local trust store
	// to be used when validating endpoint connections. This will allow Cowsql to connect to ourselves.
	err = gateway.State().UpdateAuthorizer(context.TODO())
	if err != nil {
		return nil, err
	}

	// Issue a fresh heartbeat to get the global trust stores updated.
	_ = gateway.HeartbeatRestart()

	return nodes, nil
}

// Join makes a non-clustered server join an existing cluster.
//
// It's assumed that Accept() was previously called against the leader node,
// which handed the raft server ID.
//
// The cert parameter must contain the keypair/CA material of the cluster being
// joined.
func Join[T any](gateway cluster.Gateway, networkKeypair tls.Certificate, name string, raftNodes []db.RaftNode) error {
	s := gateway.State()
	// Check parameters
	if name == "" {
		return errors.New("Member name must not be empty")
	}

	var localClusterAddress string

	err := transaction.Do(context.TODO(), gateway.Node(), func(ctx context.Context) error {
		tx := gateway.Node()
		// Fetch current network address and raft nodes
		var err error

		localClusterAddress, err = tx.GetClusterAddress(ctx)
		if err != nil {
			return err
		}

		// Make sure node-local database state is in order.
		err = membershipCheckNodeStateForBootstrapOrJoin(ctx, tx, localClusterAddress)
		if err != nil {
			return err
		}

		// Set the raft nodes list to the one that was returned by Accept().
		err = tx.ReplaceRaftNodes(ctx, raftNodes)
		if err != nil {
			return fmt.Errorf("Failed to set raft nodes: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	var clusterResources T

	external, ok := gateway.Cluster().(db.ClusterExternal[T])
	if ok {
		err = transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
			var err error

			clusterResources, err = external.GetLocalResources(ctx)

			return err
		})
		if err != nil {
			return err
		}
	}

	// Lock regular access to the cluster database since we don't want any
	// other database code to run while we're reconfiguring raft.
	err = gateway.Cluster().EnterExclusive()
	if err != nil {
		return fmt.Errorf("Failed to acquire cluster database lock: %w", err)
	}

	// Shutdown the gateway and wipe any raft data. This will trash any
	// gRPC SQL connection against our in-memory cowsql driver and shutdown
	// the associated raft instance.
	err = gateway.ShutdownServer()
	if err != nil {
		return fmt.Errorf("Failed to shutdown gRPC SQL gateway: %w", err)
	}

	err = os.RemoveAll(gateway.Node().GlobalDatabaseDir())
	if err != nil {
		return fmt.Errorf("Failed to remove existing raft data: %w", err)
	}

	networkCert, err := gateway.State().SetClusterCertificate(cowsqltls.NewCertInfo(networkKeypair, nil, nil))
	if err != nil {
		return err
	}

	// Re-initialize the gateway. This will create a new raft factory an
	// cowsql driver instance, which will be exposed over gRPC by the
	// gateway handlers.
	gateway.NetworkUpdateCert(networkCert)

	err = gateway.Initialize(false)
	if err != nil {
		return fmt.Errorf("Failed to re-initialize gRPC SQL gateway: %w", err)
	}

	// If we are listed among the database nodes, join the raft cluster.
	var info *db.RaftNode

	for _, raftNode := range raftNodes {
		if raftNode.Address == localClusterAddress {
			info = &raftNode
		}
	}

	if info == nil {
		return errors.New("Joining member info not found")
	}

	slog.Info("Joining cowsql raft cluster", "id", info.ID, "local", info.Address, "role", info.Role)

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
			slog.Warn("Failed to close client")
		}
	}()

	slog.Info("Adding node to cluster", "id", info.ID, "local", info.Address, "role", info.Role)

	ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Repeatedly try to join in case the cluster is busy with a role-change.
	joined := false
	for !joined {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Failed to join cluster: %w", ctx.Err())
		default:
			err = cowsqlClient.Add(ctx, client.NodeInfo{ID: info.ID, Address: info.Address, Role: info.Role})
			if err != nil && err.Error() == errClusterBusy.Error() {
				// If the cluster is busy with a role change, sleep a second and then keep trying to join.
				time.Sleep(1 * time.Second)

				continue
			}

			if err != nil {
				return fmt.Errorf("Failed to join cluster: %w", err)
			}

			joined = true
		}
	}

	// Make sure we can actually connect to the cluster database through
	// the network endpoint. This also releases the previously acquired
	// lock and makes the Go SQL pooling system invalidate the old
	// connection, so new queries will be executed over the new gRPC
	// network connection. Also, update the storage_pools and networks
	// tables with our local configuration.
	slog.Info("Migrate local data to cluster database")

	err = transaction.DoExclusive(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()

		node, err := tx.GetNodeByAddress(ctx, localClusterAddress, true)
		if err != nil {
			return fmt.Errorf("Failed to get ID of joining node from address %q: %w", localClusterAddress, err)
		}

		rtx, ok := tx.(db.ClusterExternal[T])
		if ok {
			err = rtx.UpdateClusterResources(ctx, node, clusterResources)
			if err != nil {
				return err
			}
		}

		// Remove the pending flag for ourselves
		// notifications.
		err = tx.SetNodePendingFlag(ctx, node.ID, false)
		if err != nil {
			return fmt.Errorf("Failed to unmark the node as pending: %w", err)
		}

		// Set last heartbeat time to now, as member is clearly online as it just successfully joined,
		// that way when we send the notification to all members below it will consider this member online.
		err = tx.SetNodeHeartbeat(ctx, node.Address, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("Failed setting last heartbeat time for member: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("(Join) Cluster database initialization failed: %w", err)
	}

	// Generate partial heartbeat request containing just a raft node list.
	if s.HasListener(context.TODO()) == nil {
		NotifyHeartbeat(gateway)
	}

	return nil
}

// NotifyHeartbeat attempts to send a heartbeat to all other members to notify them of a new or changed member.
func NotifyHeartbeat(gateway cluster.Gateway) {
	s := gateway.State()
	// If a heartbeat round is already running (and implicitly this means we are the leader), then cancel it
	// so we can distribute the fresh member state info.
	heartbeatCancel := gateway.HeartbeatCancelFunc()
	if heartbeatCancel != nil {
		heartbeatCancel()
		gateway.AwaitHeartbeat()
	}

	hbState := heartbeat.NewAPIHearbeat(gateway.Cluster())
	hbState.Time = time.Now().UTC()

	var (
		err                 error
		raftNodes           []db.RaftNode
		localClusterAddress string
	)

	err = transaction.Do(context.TODO(), gateway.Node(), func(ctx context.Context) error {
		tx := gateway.Node()

		raftNodes, err = tx.GetRaftNodes(ctx)
		if err != nil {
			return err
		}

		localClusterAddress, err = tx.GetClusterAddress(ctx)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		slog.Warn("Failed to get current raft members", "err", err, "local", localClusterAddress)

		return
	}

	var members []db.NodeInfo

	err = transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()
		members, err = tx.GetNodes(ctx)

		return err
	})
	if err != nil {
		slog.Warn("Failed to get current cluster members", "err", err, "local", localClusterAddress)

		return
	}

	// Setup a full-state notification heartbeat.
	hbState.Update(true, raftNodes, members, gateway.HeartbeatOfflineThreshold())

	var wg sync.WaitGroup

	// Refresh local event listeners.
	wg.Go(func() {
		var hbMembers map[int64]db.HeartbeatMember

		if len(hbState.Members) == 0 {
			var (
				err              error
				members          []db.NodeInfo
				offlineThreshold time.Duration
			)

			err = transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
				tx := gateway.Cluster()

				members, err = tx.GetNodes(ctx)
				if err != nil {
					return err
				}

				offlineThreshold, err = tx.GetNodeOfflineThreshold(ctx)
				if err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				slog.Warn("Failed to get current cluster members", "err", err)

				return
			}

			hbMembers = make(map[int64]db.HeartbeatMember, len(members))
			for _, member := range members {
				hbMembers[member.ID] = db.HeartbeatMember{
					ID:            member.ID,
					Name:          member.Name,
					Address:       member.Address,
					LastHeartbeat: member.Heartbeat,
					Online:        !member.IsOffline(offlineThreshold),
					Roles:         member.Roles,
				}
			}
		} else {
			hbMembers = make(map[int64]db.HeartbeatMember, len(hbState.Members))
			maps.Copy(hbMembers, hbState.Members)
		}

		s.OnHeartbeatNotification(hbMembers)
	})

	// Notify all other members of the change in membership.
	slog.Info("Notifying cluster members of local role change")

	for _, member := range members {
		if member.Address == localClusterAddress {
			continue
		}

		wg.Add(1)

		go func(address string) {
			_ = heartbeat.SendNodeHeartbeat(context.Background(), gateway.Options().RestrictTLS(), gateway.Options().DatabaseEndpoint(), address, gateway.NetworkCert(), gateway.ServerCert(), hbState)

			wg.Done()
		}(member.Address)
	}

	// Wait until all members have been notified (or at least have had a change to be notified).
	wg.Wait()
}

// Rebalance adjusts raft node roles to maintain cluster membership limits.
//
//   - Incus nodes with the 'database-client' role are mapped to the raft
//     'spare' role during rebalancing.
//   - If we are below membershipMaxRaftVoters, promote a node to 'voter'.
//   - If we are below membershipMaxStandBys, promote a node to 'standby'.
//
// Returns:
// - the address of the node that was promoted or demoted (if any).
// - and the full list of raft nodes after rebalancing.
func Rebalance(gateway cluster.Gateway, unavailableMembers []string, alwaysDemoteRoles []string) (string, []db.RaftNode, error) {
	// If we're a standalone node, do nothing.
	if gateway.Standalone() {
		return "", nil, nil
	}

	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	if err != nil {
		return "", nil, fmt.Errorf("Get current raft nodes: %w", err)
	}

	var members []db.NodeInfo

	err = transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		var err error

		tx := gateway.Cluster()
		members, err = tx.GetNodes(ctx)

		return err
	})
	if err != nil {
		return "", nil, fmt.Errorf("Get current members: %w", err)
	}

	// Prepare a map that stores node information by address.
	membersInfo := map[string]db.NodeInfo{}
	for _, member := range members {
		membersInfo[member.Address] = member
	}

	for i, n := range nodes {
		// If no member has this address, continue searching. This should not happen.
		member, ok := membersInfo[n.Address]
		if !ok {
			continue
		}

		// Check if the node has the 'database-client' role.
		var hasAnyDemoteRole bool

		for _, r := range alwaysDemoteRoles {
			if slices.Contains(member.Roles, r) {
				hasAnyDemoteRole = true

				break
			}
		}

		if !hasAnyDemoteRole || len(alwaysDemoteRoles) == 0 {
			continue
		}

		// If the node already has the 'spare' role, do nothing.
		if n.Role == db.RaftSpare {
			continue
		}

		nodes[i].Role = db.RaftSpare

		return n.Address, nodes, nil
	}

	roles, err := newRolesChanges(gateway, nodes, unavailableMembers)
	if err != nil {
		return "", nil, err
	}

	role, candidates := roles.Adjust(gateway.RaftNode().ID)

	if role == -1 {
		// No node to process.
		return "", nodes, nil
	}

	// Check if we have a spare node that we can promote to the missing role.
	candidateAddress := ""

	for _, candidate := range candidates {
		// If no member has this address, continue searching. This should not happen.
		member, ok := membersInfo[candidate.Address]
		if !ok {
			continue
		}

		// Exclude nodes with the "database-client" role from candidates.
		var hasAnyDemoteRole bool

		for _, r := range alwaysDemoteRoles {
			if slices.Contains(member.Roles, r) {
				hasAnyDemoteRole = true

				break
			}
		}

		if hasAnyDemoteRole {
			continue
		}

		candidateAddress = candidate.Address
	}

	for i, raftNode := range nodes {
		if raftNode.Address == candidateAddress {
			nodes[i].Role = role

			break
		}
	}

	return candidateAddress, nodes, nil
}

// Assign a new role to the local cowsql node.
func Assign(gateway cluster.Gateway, nodes []db.RaftNode) error {
	s := gateway.State()

	address := s.ClusterAddress()
	// Ensure we actually have an address.
	if address == "" {
		return errors.New("Cluster member is not exposed on the network")
	}

	err := transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()
		_, err := tx.GetNodeByAddress(ctx, address, false)

		return err
	})
	if err != nil {
		return fmt.Errorf("Failed to fetch the address of this cluster member: %w", err)
	}

	// Figure out our node identity.
	var info *db.RaftNode

	for i, raftNode := range nodes {
		if raftNode.Address == address {
			info = &nodes[i]

			break
		}
	}

	// Ensure that our address was actually included in the given list of raft nodes.
	if info == nil {
		return errors.New("This member is not included in the given list of database nodes")
	}

	// Replace our local list of raft nodes with the given one (which
	// includes ourselves).

	err = transaction.Do(context.TODO(), gateway.Node(), func(ctx context.Context) error {
		tx := gateway.Node()

		err := tx.ReplaceRaftNodes(ctx, nodes)
		if err != nil {
			return fmt.Errorf("Failed to set raft nodes: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	var transactor func(context.Context, transaction.Transactor, func(ctx context.Context) error) error

	if !gateway.Initialized() {
		return errors.New("Gateway has not been initialized")
	}

	transactor = transaction.Do

	slog.Info("Changing local database role", "role", info.Role)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cowsqlClient, err := client.FindLeader(ctx, gateway.NodeStore(), client.WithDialFunc(gateway.RaftDial()))
	if err != nil {
		return fmt.Errorf("Connect to cluster leader: %w", err)
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client")
		}
	}()

	// Figure out our current role.
	role := db.RaftRole(-1)

	currentCluster, err := cowsqlClient.Cluster(ctx)
	if err != nil {
		return fmt.Errorf("Fetch current cluster configuration: %w", err)
	}

	for _, server := range currentCluster {
		if server.ID == info.ID {
			role = server.Role

			break
		}
	}

	if role == -1 {
		return fmt.Errorf("Node %s does not belong to the current raft configuration", address)
	}

	// If we're stepping back from voter to spare, let's first transition
	// to stand-by first and wait for the configuration change to be
	// notified to us. This prevent us from thinking we're still voters and
	// potentially disrupt the cluster.
	if role == db.RaftVoter && info.Role == db.RaftSpare {
		err = cowsqlClient.Assign(ctx, info.ID, db.RaftStandBy)
		if err != nil {
			return fmt.Errorf("Failed to step back to stand-by: %w", err)
		}

		local, err := gateway.RaftClient(context.TODO())
		if err != nil {
			return fmt.Errorf("Failed to get local cowsql client: %w", err)
		}

		notified := false

		for range 10 {
			time.Sleep(500 * time.Millisecond)

			servers, err := local.Cluster(context.Background())
			if err != nil {
				return fmt.Errorf("Failed to get current cluster: %w", err)
			}

			for _, server := range servers {
				if server.ID != info.ID {
					continue
				}

				if server.Role == db.RaftStandBy {
					notified = true

					break
				}
			}

			if notified {
				break
			}
		}

		if !notified {
			return errors.New("Timeout waiting for configuration change notification")
		}
	}

	// Give the Assign operation a bit more budget in case we're promoting
	// to voter, since that might require a snapshot transfer.
	if info.Role == db.RaftVoter {
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
	}

	err = cowsqlClient.Assign(ctx, info.ID, info.Role)
	if err != nil {
		return fmt.Errorf("Failed to assign role %q to %q: %w", info.Role.String(), info.Name, err)
	}

	gateway.SetRaftNode(info)

	// Unlock regular access to our cluster database.
	err = transactor(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("(Assign) Cluster database initialization failed: %w", err)
	}

	// Generate partial heartbeat request containing just a raft node list.
	if s.HasListener(context.TODO()) == nil {
		NotifyHeartbeat(gateway)
	}

	return nil
}

// Leave a cluster.
//
// If the force flag is true, the node will leave even if it still has
// containers and images.
//
// The node will only leave the raft cluster, and won't be removed from the
// database. That's done by Purge().
//
// Upon success, return the address of the leaving node.
//
// This function must be called by the cluster leader.
func Leave[T any](gateway cluster.Gateway, name string, force bool, pending bool) (string, error) {
	slog.Debug("Make node leave the cluster", "name", name)

	// Check if the node can be deleted and track its address.
	var address string

	err := transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()
		// Get the node (if it doesn't exists an error is returned).
		var (
			node db.NodeInfo
			err  error
		)
		if pending {
			node, err = tx.GetNodeByName(ctx, name, true)
			if err != nil {
				return fmt.Errorf("Failed to get member %q: %w", name, err)
			}
		} else {
			node, err = tx.GetNodeByName(ctx, name, false)
			if err != nil {
				return fmt.Errorf("Failed to get member %q: %w", name, err)
			}
		}

		// Check that the node is eligeable for leaving.
		if !force {
			err := membershipCheckClusterStateForLeave[T](ctx, tx, node)
			if err != nil {
				return err
			}
		}

		address = node.Address

		return nil
	})
	if err != nil {
		return "", err
	}

	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	if err != nil {
		return "", err
	}

	var info *db.RaftNode // Raft node to remove, if any.

	for i, raftNode := range nodes {
		if raftNode.Address == address {
			info = &nodes[i]

			break
		}
	}

	if info == nil {
		// The node was not part of the raft cluster, nothing left to do.
		return address, nil
	}

	// Get the address of another database node,
	slog.Info("Remove node from cowsql raft cluster", "id", info.ID, "address", info.Address)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cowsqlClient, err := gateway.RaftClient(context.TODO())
	if err != nil {
		return "", fmt.Errorf("Failed to connect to cluster leader: %w", err)
	}

	defer func() {
		err := cowsqlClient.Close()
		if err != nil {
			slog.Warn("Failed to close client")
		}
	}()

	err = cowsqlClient.Remove(ctx, info.ID)
	if err != nil {
		return "", fmt.Errorf("Failed to leave the cluster: %w", err)
	}

	return address, nil
}

// Handover looks for a non-voter member that can be promoted to replace the
// member with the given address, which is shutting down. It returns the
// address of such member along with an updated list of nodes, with the ne role
// set.
//
// It should be called only by the current leader.
func Handover(gateway cluster.Gateway, address string) (string, []db.RaftNode, error) {
	nodes, err := gateway.CurrentRaftNodes(context.TODO())
	if err != nil {
		return "", nil, fmt.Errorf("Get current raft nodes: %w", err)
	}

	var nodeID uint64

	for _, raftNode := range nodes {
		if raftNode.Address == address {
			nodeID = raftNode.ID
		}
	}

	if nodeID == 0 {
		return "", nil, fmt.Errorf("No cowsql node has address %s: %w", address, err)
	}

	roles, err := newRolesChanges(gateway, nodes, nil)
	if err != nil {
		return "", nil, err
	}

	role, candidates := roles.Handover(nodeID)
	if role == -1 {
		return "", nil, nil
	}

	for i, raftNode := range nodes {
		if raftNode.Address == candidates[0].Address {
			nodes[i].Role = role

			return raftNode.Address, nodes, nil
		}
	}

	return "", nil, nil
}

// Build an app.RolesChanges object fed with the current cluster state.
func newRolesChanges(gateway cluster.Gateway, nodes []db.RaftNode, unavailableMembers []string) (*app.RolesChanges, error) {
	var domains map[string]uint64

	err := transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()

		var err error

		domains, err = tx.GetNodesFailureDomains(ctx)

		return err
	})
	if err != nil {
		return nil, fmt.Errorf("Load failure domains: %w", err)
	}

	clusterState := map[client.NodeInfo]*client.NodeMetadata{}

	for _, raftNode := range nodes {
		nodeInfo := client.NodeInfo{ID: raftNode.ID, Address: raftNode.Address, Role: raftNode.Role}
		if !slices.Contains(unavailableMembers, raftNode.Address) && cluster.HasConnectivity(gateway.NetworkCert(), gateway.ServerCert(), raftNode.Address, gateway.Options().RestrictTLS()) {
			clusterState[nodeInfo] = &client.NodeMetadata{
				FailureDomain: domains[raftNode.Address],
			}
		} else {
			clusterState[nodeInfo] = nil
		}
	}

	roles := &app.RolesChanges{
		Config: app.RolesConfig{
			Voters:   int(gateway.Options().MaxVotersFunc()()),
			StandBys: int(gateway.Options().MaxStandbyFunc()()),
		},
		State: clusterState,
	}

	return roles, nil
}

// Purge removes a node entirely from the cluster database.
func Purge[T any](gateway cluster.Gateway, name string, pending bool) error {
	slog.Debug("Remove node from the database", "name", name)

	return transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()
		// Get the node (if it doesn't exists an error is returned).
		var (
			node db.NodeInfo
			err  error
		)
		if pending {
			node, err = tx.GetNodeByName(ctx, name, true)
			if err != nil {
				return fmt.Errorf("Failed to get member %q: %w", name, err)
			}
		} else {
			node, err = tx.GetNodeByName(ctx, name, false)
			if err != nil {
				return fmt.Errorf("Failed to get member %q: %w", name, err)
			}
		}

		external, ok := tx.(db.ClusterExternal[T])
		if ok {
			err = external.ClearNode(ctx, node.ID)
			if err != nil {
				return fmt.Errorf("Failed to clear member %q: %w", name, err)
			}
		}

		err = tx.RemoveNode(ctx, node.Name)
		if err != nil {
			return fmt.Errorf("Failed to remove member %q: %w", name, err)
		}

		return nil
	})
}

// Count is a convenience for checking the current number of nodes in the
// cluster.
func Count(gateway cluster.Gateway) (int, error) {
	var count int

	err := transaction.Do(context.TODO(), gateway.Cluster(), func(ctx context.Context) error {
		tx := gateway.Cluster()

		var err error

		count, err = tx.GetNodesCount(ctx)

		return err
	})

	return count, err
}

// Enabled is a convenience that returns true if clustering is enabled on this
// node.
func Enabled(nodeDB db.Node) (bool, error) {
	var enabled bool

	err := transaction.Do(context.TODO(), nodeDB, func(ctx context.Context) error {
		tx := nodeDB

		addresses, err := tx.GetRaftNodes(ctx)
		if err != nil {
			return err
		}

		enabled = len(addresses) > 0

		return nil
	})

	return enabled, err
}

// Check that node-related preconditions are met for bootstrapping or joining a
// cluster.
func membershipCheckNodeStateForBootstrapOrJoin(ctx context.Context, tx db.Node, address string) error {
	nodes, err := tx.GetRaftNodes(ctx)
	if err != nil {
		return fmt.Errorf("Failed to fetch current raft nodes: %w", err)
	}

	hasClusterAddress := address != ""
	hasRaftNodes := len(nodes) > 0

	// Ensure that we're not in an inconsistent situation, where no cluster address is set, but still there
	// are entries in the raft_nodes table.
	if !hasClusterAddress && hasRaftNodes {
		return errors.New("Inconsistent state: found leftover entries in member cache")
	}

	if !hasClusterAddress {
		return errors.New("No cluster address config is set on this member")
	}

	if hasRaftNodes {
		return errors.New("The member is already part of a cluster")
	}

	return nil
}

// Check that cluster-related preconditions are met for bootstrapping or
// joining a cluster.
func membershipCheckClusterStateForBootstrapOrJoin(ctx context.Context, tx db.Cluster) error {
	members, err := tx.GetNodes(ctx)
	if err != nil {
		return fmt.Errorf("Failed getting cluster members: %w", err)
	}

	if len(members) != 1 {
		return errors.New("Inconsistent state: Found leftover cluster members")
	}

	return nil
}

// Check that cluster-related preconditions are met for accepting a new node.
func membershipCheckClusterStateForAccept(ctx context.Context, version string, tx db.Cluster, name string, address string, schema int, api int) error {
	members, err := tx.GetNodes(ctx)
	if err != nil {
		return fmt.Errorf("Failed getting cluster members: %w", err)
	}

	if len(members) == 1 && members[0].Address == "0.0.0.0" {
		return errors.New("Clustering isn't enabled")
	}

	for _, member := range members {
		if member.Name == name {
			return fmt.Errorf("The cluster already has a member with name: %s", name)
		}

		if member.Address == address {
			return fmt.Errorf("The cluster already has a member with address: %s", address)
		}

		if member.Schema != schema {
			return fmt.Errorf("The joining server version doesn't match (expected %s with DB schema %d, got %d)", version, member.Schema, schema)
		}

		if member.APIExtensions != api {
			return fmt.Errorf("The joining server version doesn't match (expected %s with API count %v)", version, api)
		}
	}

	return nil
}

// Check that cluster-related preconditions are met for leaving a cluster.
func membershipCheckClusterStateForLeave[T any](ctx context.Context, tx db.Cluster, node db.NodeInfo) error {
	// Check that it has no containers or images.
	external, ok := tx.(db.ClusterExternal[T])
	if ok {
		message, err := external.NodeIsEmpty(ctx, node.ID)
		if err != nil {
			return err
		}

		if message != "" {
			return errors.New(message)
		}
	}

	// Check that it's not the last member.
	members, err := tx.GetNodes(ctx)
	if err != nil {
		return fmt.Errorf("Failed getting cluster members: %w", err)
	}

	if len(members) == 1 {
		return errors.New("Member is the only member in the cluster")
	}

	return nil
}
