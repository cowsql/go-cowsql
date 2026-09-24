//go:build !nosqlite3

package example_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/membership"
	"github.com/cowsql/go-cowsql/cluster/options"
)

type outdatedCluster struct {
	db.Cluster
}

// NodeIsOutdated will always return true for outdatedCluster.
func (c *outdatedCluster) NodeIsOutdated(ctx context.Context) (bool, error) {
	return true, nil
}

// The task function checks if the node is out of date and runs whatever is in
// INCUS_CLUSTER_UPDATE if so.
func TestMaybeUpdate_Upgrade(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	var triggered bool

	preUpdateCheck := options.PreUpdateCheck(func() (func() error, error) {
		return func() error {
			triggered = true
			return nil
		}, nil
	})

	_, member1, _ := f.node(preUpdateCheck)
	err := membership.Bootstrap(member1, "member1")
	require.NoError(f.t, err)
	f.Grow()
	member1.SetClusterDB(&outdatedCluster{Cluster: member1.Cluster()})

	dir := f.t.TempDir()

	defer func() { _ = os.RemoveAll(dir) }()

	require.NoError(t, membership.MaybeUpdate(member1))
	require.True(t, triggered)
}

// If the node is up-to-date, nothing is done.
func TestMaybeUpdate_NothingToDo(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	var triggered bool

	preUpdateCheck := options.PreUpdateCheck(func() (func() error, error) {
		return func() error {
			triggered = true
			return nil
		}, nil
	})

	_, member1, _ := f.node(preUpdateCheck)
	err := membership.Bootstrap(member1, "member1")
	require.NoError(f.t, err)
	f.Grow()

	dir := f.t.TempDir()

	defer func() { _ = os.RemoveAll(dir) }()

	require.NoError(t, membership.MaybeUpdate(member1))
	require.False(t, triggered)
}
