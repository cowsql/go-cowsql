//go:build !nosqlite3

package example_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/internal/util/file"
	"github.com/cowsql/go-cowsql/cluster/membership"
)

func TestBootstrap_UnmetPreconditions(t *testing.T) {
	cases := []struct {
		setup func(*membershipFixtures)
		error string
	}{
		{
			func(f *membershipFixtures) {
				f.ClusterAddress("")
			},
			"No cluster address config is set on this member",
		},
		{
			func(f *membershipFixtures) {
				f.ClusterAddress("1.2.3.4:666")
				f.RaftNode("5.6.7.8:666")
			},
			"The member is already part of a cluster",
		},
		{
			func(f *membershipFixtures) {
				f.ClusterAddress("")
				f.RaftNode("5.6.7.8:666")
			},
			"Inconsistent state: found leftover entries in member cache",
		},
		{
			func(f *membershipFixtures) {
				f.ClusterAddress("1.2.3.4:666")
				f.ClusterNode("5.6.7.8:666")
			},
			"Inconsistent state: Found leftover cluster members",
		},
	}

	for _, c := range cases {
		t.Run(c.error, func(t *testing.T) {
			f := heartbeatFixture{t: t}
			defer f.Cleanup()

			_, gateway, _ := f.node()

			c.setup(&membershipFixtures{t: t, gateway: gateway})

			err := membership.Bootstrap(gateway, "buzz")
			require.EqualError(t, err, c.error)
		})
	}
}

func TestBootstrap(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	_, gateway, address := f.node()

	err := membership.Bootstrap(gateway, "buzz")
	require.NoError(t, err)

	// The node-local database has now an entry in the raft_nodes table
	err = transaction.ForceTx(t.Context(), gateway.Node(), func(ctx context.Context, tx transaction.TX) error {
		nodes, err := gateway.Node().GetRaftNodes(ctx)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Equal(t, uint64(1), nodes[0].ID)
		require.Equal(t, address, nodes[0].Address)

		return nil
	})
	require.NoError(t, err)

	// The cluster database has now an entry in the nodes table
	err = transaction.ForceTx(t.Context(), gateway.Cluster(), func(ctx context.Context, tx transaction.TX) error {
		members, err := gateway.Cluster().GetNodes(ctx)
		require.NoError(t, err)
		require.Len(t, members, 1)
		require.Equal(t, "buzz", members[0].Name)
		require.Equal(t, address, members[0].Address)

		return nil
	})
	require.NoError(t, err)

	// The cluster certificate is in place.
	require.True(t, file.PathExists(filepath.Join(filepath.Dir(filepath.Dir(gateway.Node().GlobalDatabaseDir())), "cluster.crt")))

	count, err := membership.Count(gateway)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	enabled, err := membership.Enabled(gateway.Node())
	require.NoError(t, err)
	require.True(t, enabled)
}

// If pre-conditions are not met, a descriptive error is returned.
func TestAccept_UnmetPreconditions(t *testing.T) {
	cases := []struct {
		name      string
		address   string
		schema    int
		api       int
		bootstrap bool
		error     string
	}{
		{
			"member2",
			"1.2.3.4:666",
			1,
			1,
			false,
			"Clustering isn't enabled",
		},
		{
			"buzz",
			"1.2.3.4:666",
			1,
			1,
			true,
			"The cluster already has a member with name: buzz",
		},
		{
			"member2",
			"", // re-use member1 address
			1,
			1,
			true,
			"The cluster already has a member with address:",
		},
		{
			"member2",
			"1.2.3.4:666",
			2,
			1,
			true,
			fmt.Sprintf("The joining server version doesn't match (expected %s with DB schema %d, got %d)", "0.0.0", 1, 2),
		},
		{
			"member2",
			"1.2.3.4:666",
			1,
			2,
			true,
			fmt.Sprintf("The joining server version doesn't match (expected %s with API count %d)", "0.0.0", 2),
		},
	}

	for _, c := range cases {
		t.Run(c.error, func(t *testing.T) {
			f := heartbeatFixture{t: t}
			defer f.Cleanup()

			var gateway cluster.Gateway
			if c.bootstrap {
				gateway = f.Bootstrap()
			} else {
				_, gateway, _ = f.node()
			}

			address := c.address
			checkErr := c.error

			if address == "" {
				address = gateway.RaftNode().Address
				checkErr = checkErr + " " + address
			}

			serverCertx509, err := gateway.ServerCert().PublicKeyX509()
			require.NoError(t, err)

			_, err = membership.Accept(gateway, serverCertx509, c.name, address, c.schema, c.api, 1)
			require.EqualError(t, err, checkErr)
		})
	}
}

// When a node gets accepted, it gets included in the raft nodes.
func TestAccept(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	gateway := f.Bootstrap()

	serverCertx509, err := gateway.ServerCert().PublicKeyX509()
	require.NoError(t, err)

	nodes, err := membership.Accept(gateway, serverCertx509, "member2", "5.6.7.8:666", 1, 1, 1)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, uint64(1), nodes[0].ID)
	require.Equal(t, uint64(2), nodes[1].ID)
	require.Equal(t, gateway.RaftNode().Address, nodes[0].Address)
	require.Equal(t, "5.6.7.8:666", nodes[1].Address)
}

func TestJoin(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	gateway := f.Bootstrap()
	member1 := f.Grow()

	// The leader now returns an updated list of raft nodes.
	// The new node is not included to ensure distributed consensus.
	raftNodes, err := gateway.CurrentRaftNodes(context.TODO())
	require.NoError(t, err)
	require.Len(t, raftNodes, 2)
	require.Equal(t, uint64(1), raftNodes[0].ID)
	require.Equal(t, gateway.RaftNode().Address, raftNodes[0].Address)
	require.Equal(t, db.RaftVoter, raftNodes[0].Role)
	require.Equal(t, uint64(2), raftNodes[1].ID)
	require.Equal(t, member1.RaftNode().Address, raftNodes[1].Address)
	require.Equal(t, db.RaftStandBy, raftNodes[1].Role)

	// The Count function returns the number of nodes.
	count, err := membership.Count(gateway)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Leave the cluster.
	leaveName := member1.RaftNode().Name
	leaveAddress := member1.RaftNode().Address
	leaving, err := membership.Leave[struct{}](gateway, leaveName, false /* force */, false)
	require.NoError(t, err)
	require.Equal(t, leaveAddress, leaving)

	err = membership.Purge[struct{}](gateway, leaveName, false)
	require.NoError(t, err)

	// The node has gone from the cluster db.
	members, err := gateway.Cluster().GetNodes(context.TODO())
	require.NoError(t, err)
	require.Len(t, members, 1)

	// The node has gone from the raft cluster.
	raftMembers, err := gateway.CurrentRaftNodes(context.TODO())
	require.NoError(t, err)
	require.Len(t, raftMembers, 1)
}

// Helper for setting fixtures for Bootstrap tests.
type membershipFixtures struct {
	t       *testing.T
	gateway cluster.Gateway
}

// Set cluster.https_address to the given value.
func (h *membershipFixtures) ClusterAddress(address string) {
	err := transaction.ForceTx(h.t.Context(), h.gateway.Node(), func(ctx context.Context, tx transaction.TX) error {
		return h.gateway.Node().SetClusterAddress(ctx, address)
	})
	require.NoError(h.t, err)
}

// Add the given address to the raft_nodes table.
func (h *membershipFixtures) RaftNode(address string) {
	setRaftRole(h.t, h.gateway.Node(), address)
}

// Get the current list of the raft nodes in the raft_nodes table.
func (h *membershipFixtures) RaftNodes() []db.RaftNode {
	var nodes []db.RaftNode

	err := transaction.ForceTx(h.t.Context(), h.gateway.Node(), func(ctx context.Context, tx transaction.TX) error {
		var err error

		nodes, err = h.gateway.Node().GetRaftNodes(ctx)

		return err
	})
	require.NoError(h.t, err)

	return nodes
}

// Add the given address to the nodes table of the cluster database.
func (h *membershipFixtures) ClusterNode(address string) {
	err := transaction.ForceTx(h.t.Context(), h.gateway.Cluster(), func(ctx context.Context, tx transaction.TX) error {
		_, err := h.gateway.Cluster().CreateNode(ctx, "test", address, 1)
		return err
	})
	require.NoError(h.t, err)
}
