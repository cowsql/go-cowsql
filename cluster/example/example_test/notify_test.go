//go:build !nosqlite3

package example_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cowsql/go-cowsql/cluster"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
)

// The returned notifier connects to all nodes.
func TestNewNotifier(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	member1 := f.Bootstrap()
	member2 := f.Grow()
	member3 := f.Grow()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	notifier, err := member1.NewNotifier(ctx, member1.NetworkCert(), member1.ServerCert(), cluster.NotifyAll)
	require.NoError(t, err)

	peers := make(chan string, 2)
	hook := func(ctx context.Context, address string, networkCert, serverCert cowsqltls.CertInfo) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address, nil)
		require.NoError(t, err)

		c := http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
			},
		}

		resp, err := c.Do(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		peers <- string(b)

		return nil
	}

	require.Empty(t, notifier(hook))

	addresses := make([]string, 2)
	for i := range addresses {
		select {
		case addresses[i] = <-peers:
		case <-ctx.Done():
			require.FailNow(t, "Timed out waiting for request")
		}
	}

	members := []string{member2.RaftNode().Address, member3.RaftNode().Address}
	slices.Sort(members)
	slices.Sort(addresses)

	require.Equal(t, addresses, members)
}

// Creating a new notifier fails if the policy is set to NotifyAll and one of
// the nodes is down.
func TestNewNotify_NotifyAllError(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	member1 := f.Bootstrap()
	member2 := f.Grow()
	_ = f.Grow()

	require.NoError(t, member1.Cluster().SetNodeHeartbeat(t.Context(), member2.RaftNode().Address, time.Time{}))
	f.servers[member2].Close()

	notifier, err := member1.NewNotifier(t.Context(), member1.NetworkCert(), member1.ServerCert(), cluster.NotifyAll)
	require.Nil(t, notifier)
	require.Error(t, err)
	require.Regexp(t, "peer node .+ is down", err.Error())
}

// Creating a new notifier does not fail if the policy is set to NotifyAlive
// and one of the nodes is down, however dead nodes are ignored.
func TestNewNotify_NotifyAlive(t *testing.T) {
	f := heartbeatFixture{t: t}
	defer f.Cleanup()

	member1 := f.Bootstrap()
	member2 := f.Grow()
	_ = f.Grow()

	require.NoError(t, member1.Cluster().SetNodeHeartbeat(t.Context(), member2.RaftNode().Address, time.Time{}))
	f.servers[member2].Close()

	notifier, err := member1.NewNotifier(t.Context(), member1.NetworkCert(), member1.ServerCert(), cluster.NotifyAlive)
	require.NoError(t, err)

	i := 0
	hook := func(ctx context.Context, address string, networkCert, serverCert cowsqltls.CertInfo) error {
		i++
		return nil
	}

	require.Empty(t, notifier(hook))
	require.Equal(t, 1, i)
}
