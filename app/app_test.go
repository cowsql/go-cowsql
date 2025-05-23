package app_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cowsql/go-cowsql"
	"github.com/cowsql/go-cowsql/app"
	"github.com/cowsql/go-cowsql/client"
)

var (
	requireNoError = assertNoError
	requireEqual   = assertEqual
	requireTrue    = assertTrue
)

func requireNotNil(t *testing.T, x any) {
	t.Helper()
	if x == nil {
		t.Fatal()
	}
}

func assertTrue(t *testing.T, ok bool) {
	t.Helper()
	if !ok {
		t.Fatal(ok)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if expected == nil || actual == nil {
		if expected != actual {
			t.Fatal(expected, actual)
		}
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatal(expected, actual)
	}
}

func assertNotEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if expected == nil || actual == nil {
		if expected == actual {
			t.Fatal(expected, actual)
		}
	}

	if reflect.DeepEqual(expected, actual) {
		t.Fatal(expected, actual)
	}
}

// Create a pristine bootstrap node with default value.
func TestNew_PristineDefault(t *testing.T) {
	_, cleanup := newApp(t, app.WithAddress("127.0.0.1:9000"))
	defer cleanup()
}

// Create a pristine joining node.
func TestNew_PristineJoiner(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	app2, cleanup := newApp(t, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	defer cleanup()

	requireNoError(t, app2.Ready(context.Background()))

	// The joining node to appear in the cluster list.
	cli, err := app1.Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)
	assertEqual(t, addr1, cluster[0].Address)
	assertEqual(t, addr2, cluster[1].Address)

	// Initially the node joins as spare.
	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Spare, cluster[1].Role)
}

// Restart a node that had previously joined the cluster successfully.
func TestNew_JoinerRestart(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	requireNoError(t, app1.Ready(context.Background()))

	dir2, cleanup := newDir(t)
	defer cleanup()

	app2, cleanup := newAppWithDir(t, dir2, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	requireNoError(t, app2.Ready(context.Background()))
	cleanup()

	app2, cleanup = newAppWithDir(t, dir2, app.WithAddress(addr2))
	defer cleanup()

	requireNoError(t, app2.Ready(context.Background()))
}

// The second joiner promotes itself and also the first joiner.
func TestNew_SecondJoiner(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"
	addr3 := "127.0.0.1:9003"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	app2, cleanup := newApp(t, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	defer cleanup()

	requireNoError(t, app2.Ready(context.Background()))

	app3, cleanup := newApp(t, app.WithAddress(addr3), app.WithCluster([]string{addr1}))
	defer cleanup()

	requireNoError(t, app3.Ready(context.Background()))

	cli, err := app1.Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, addr1, cluster[0].Address)
	assertEqual(t, addr2, cluster[1].Address)
	assertEqual(t, addr3, cluster[2].Address)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
}

// The third joiner gets the stand-by role.
func TestNew_ThirdJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := range 4 {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
}

// The fourth joiner gets the stand-by role.
func TestNew_FourthJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := range 5 {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.StandBy, cluster[4].Role)
}

// The fifth joiner gets the stand-by role.
func TestNew_FifthJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := range 6 {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.StandBy, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
}

// The sixth joiner gets the spare role.
func TestNew_SixthJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := range 7 {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.StandBy, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
	assertEqual(t, client.Spare, cluster[6].Role)
}

// Transfer voting rights to another online node.
func TestHandover_Voter(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)

	requireNoError(t, apps[2].Handover(context.Background()))

	cluster, err = cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Spare, cluster[2].Role)
	assertEqual(t, client.Voter, cluster[3].Role)
}

// In a two-node cluster only one of them is a voter. When Handover() is called
// on the voter, the role and leadership are transfered.
func TestHandover_TwoNodes(t *testing.T) {
	n := 2
	apps := make([]*app.App, n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	err := apps[0].Handover(context.Background())
	requireNoError(t, err)

	cli, err := apps[1].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Spare, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
}

// Transfer voting rights to another online node. Failure domains are taken
// into account.
func TestHandover_VoterHonorFailureDomain(t *testing.T) {
	n := 6
	apps := make([]*app.App, n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithFailureDomain(uint64(i % 3)),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	requireNoError(t, apps[2].Handover(context.Background()))

	cluster, err = cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Spare, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.StandBy, cluster[4].Role)
	assertEqual(t, client.Voter, cluster[5].Role)
}

// Handover with a sinle node.
func TestHandover_SingleNode(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	app, err := app.New(dir, app.WithAddress("127.0.0.1:9001"))
	requireNoError(t, err)

	requireNoError(t, app.Ready(context.Background()))

	requireNoError(t, app.Handover(context.Background()))
	requireNoError(t, app.Close())
}

// Exercise a sequential graceful shutdown of a 3-node cluster.
func TestHandover_GracefulShutdown(t *testing.T) {
	n := 3
	apps := make([]*app.App, n)

	for i := range n {
		dir, cleanup := newDir(t)
		defer cleanup()

		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, err := app.New(dir, options...)
		requireNoError(t, err)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	db, err := sql.Open(apps[0].Driver(), "test.db")
	requireNoError(t, err)

	_, err = db.Exec("CREATE TABLE test (n INT)")
	requireNoError(t, err)

	requireNoError(t, db.Close())

	requireNoError(t, apps[0].Handover(context.Background()))
	requireNoError(t, apps[0].Close())

	requireNoError(t, apps[1].Handover(context.Background()))
	requireNoError(t, apps[1].Close())

	requireNoError(t, apps[2].Handover(context.Background()))
	requireNoError(t, apps[2].Close())
}

// Transfer the stand-by role to another online node.
func TestHandover_StandBy(t *testing.T) {
	n := 7
	apps := make([]*app.App, n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.StandBy, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
	assertEqual(t, client.Spare, cluster[6].Role)

	requireNoError(t, apps[4].Handover(context.Background()))

	cluster, err = cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.Spare, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
	assertEqual(t, client.StandBy, cluster[6].Role)
}

// Transfer leadership and voting rights to another node.
func TestHandover_TransferLeadership(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	leader, err := cli.Leader(context.Background())
	requireNoError(t, err)

	requireNotNil(t, leader)
	requireEqual(t, apps[0].ID(), leader.ID)
	requireNoError(t, apps[0].Handover(context.Background()))

	cli, err = apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	leader, err = cli.Leader(context.Background())
	requireNoError(t, err)

	assertNotEqual(t, apps[0].ID(), leader.ID)

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Spare, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.Voter, cluster[3].Role)
}

// If a voter goes offline, another node takes its place.
func TestRolesAdjustment_ReplaceVoter(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(2 * time.Second),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[3]()

	// A voter goes offline.
	cleanups[2]()

	time.Sleep(8 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Spare, cluster[2].Role)
	assertEqual(t, client.Voter, cluster[3].Role)
}

// If a voter goes offline, another node takes its place. If possible, pick a
// voter from a failure domain which differs from the one of the two other
// voters.
func TestRolesAdjustment_ReplaceVoterHonorFailureDomain(t *testing.T) {
	n := 6
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(4 * time.Second),
			app.WithFailureDomain(uint64(i % 3)),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[3]()
	defer cleanups[4]()
	defer cleanups[5]()

	// A voter in failure domain 2 goes offline.
	cleanups[2]()

	time.Sleep(18 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	// The replacement was picked in the same failure domain.
	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Spare, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.StandBy, cluster[4].Role)
	assertEqual(t, client.Voter, cluster[5].Role)
}

// If a voter goes offline, another node takes its place. Preference will be
// given to candidates with lower weights.
func TestRolesAdjustment_ReplaceVoterHonorWeight(t *testing.T) {
	n := 6
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(4 * time.Second),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[3]()
	defer cleanups[4]()
	defer cleanups[5]()

	// A voter in failure domain 2 goes offline.
	cleanups[2]()

	cli, err := apps[3].Client(context.Background())
	requireNoError(t, err)
	requireNoError(t, cli.Weight(context.Background(), uint64(15)))
	defer cli.Close()

	cli, err = apps[4].Client(context.Background())
	requireNoError(t, err)
	requireNoError(t, cli.Weight(context.Background(), uint64(5)))
	defer cli.Close()

	cli, err = apps[5].Client(context.Background())
	requireNoError(t, err)
	requireNoError(t, cli.Weight(context.Background(), uint64(10)))
	defer cli.Close()

	time.Sleep(18 * time.Second)

	cli, err = apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	// The stand-by with the lowest weight was picked.
	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Spare, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.Voter, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
}

// If a voter goes offline, but no another node can its place, then nothing
// chagnes.
func TestRolesAdjustment_CantReplaceVoter(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(4 * time.Second),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()

	// A voter and a spare go offline.
	cleanups[3]()
	cleanups[2]()

	time.Sleep(12 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
}

// If a stand-by goes offline, another node takes its place.
func TestRolesAdjustment_ReplaceStandBy(t *testing.T) {
	n := 7
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(5 * time.Second),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[2]()
	defer cleanups[3]()
	defer cleanups[5]()
	defer cleanups[6]()

	// A stand-by goes offline.
	cleanups[4]()

	time.Sleep(20 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.Spare, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
	assertEqual(t, client.StandBy, cluster[6].Role)
}

// If a stand-by goes offline, another node takes its place. If possible, pick
// a stand-by from a failure domain which differs from the one of the two other
// stand-bys.
func TestRolesAdjustment_ReplaceStandByHonorFailureDomains(t *testing.T) {
	n := 9
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := range n {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(5 * time.Second),
			app.WithFailureDomain(uint64(i % 3)),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		requireNoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[2]()
	defer cleanups[3]()
	defer cleanups[5]()
	defer cleanups[6]()
	defer cleanups[7]()
	defer cleanups[8]()

	// A stand-by from failure domain 1 goes offline.
	cleanups[4]()

	time.Sleep(20 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)

	// The replacement was picked in the same failure domain.
	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
	assertEqual(t, client.StandBy, cluster[3].Role)
	assertEqual(t, client.Spare, cluster[4].Role)
	assertEqual(t, client.StandBy, cluster[5].Role)
	assertEqual(t, client.Spare, cluster[6].Role)
	assertEqual(t, client.StandBy, cluster[7].Role)
	assertEqual(t, client.Spare, cluster[8].Role)
}

// Open a database on a fresh one-node cluster.
func TestOpen(t *testing.T) {
	app, cleanup := newApp(t, app.WithAddress("127.0.0.1:9000"))
	defer cleanup()

	db, err := app.Open(context.Background(), "test")
	requireNoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "CREATE TABLE foo(n INT)")
	assertNoError(t, err)
}

// Test some setup options
func TestOptions(t *testing.T) {
	options := []app.Option{
		app.WithAddress("127.0.0.1:9000"),
		app.WithNetworkLatency(20 * time.Millisecond),
		app.WithSnapshotParams(cowsql.SnapshotParams{Threshold: 1024, Trailing: 1024}),
		app.WithTracing(client.LogDebug),
	}
	app, cleanup := newApp(t, options...)
	defer cleanup()
	requireNotNil(t, app)
}

// Test client connections dropping uncleanly.
func TestProxy_Error(t *testing.T) {
	cert, pool := loadCert(t)
	dial := client.DialFuncWithTLS(client.DefaultDialFunc, app.SimpleDialTLSConfig(cert, pool))

	_, cleanup := newApp(t, app.WithAddress("127.0.0.1:9000"))
	defer cleanup()

	// Simulate a client which writes the protocol header, then a Leader
	// request and finally drops before reading the response.
	conn, err := dial(context.Background(), "127.0.0.1:9000")
	requireNoError(t, err)

	protocol := make([]byte, 8)
	binary.LittleEndian.PutUint64(protocol, uint64(1))

	n, err := conn.Write(protocol)
	requireNoError(t, err)
	assertEqual(t, n, 8)

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:], 1)
	header[4] = 0
	header[5] = 0
	binary.LittleEndian.PutUint16(header[6:], 0)

	n, err = conn.Write(header)
	requireNoError(t, err)
	assertEqual(t, n, 8)

	body := make([]byte, 8)
	n, err = conn.Write(body)
	requireNoError(t, err)
	assertEqual(t, n, 8)

	time.Sleep(100 * time.Millisecond)
	conn.Close()
	time.Sleep(250 * time.Millisecond)
}

// If the given context is cancelled before initial tasks are completed, an
// error is returned.
func TestReady_Cancel(t *testing.T) {
	app, cleanup := newApp(t, app.WithAddress("127.0.0.1:9002"), app.WithCluster([]string{"127.0.0.1:9001"}))
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := app.Ready(ctx)

	assertEqual(t, ctx.Err(), err)
}

func newApp(t *testing.T, options ...app.Option) (*app.App, func()) {
	t.Helper()

	dir, dirCleanup := newDir(t)

	app, appCleanup := newAppWithDir(t, dir, options...)

	cleanup := func() {
		appCleanup()
		dirCleanup()
	}

	return app, cleanup
}

// TestExternalConn creates a 3-member cluster using external http connections
// and ensures the cluster is successfully created, and that the connection is
// handled manually.
func TestExternalConnWithTCP(t *testing.T) {
	externalAddr1 := "127.0.0.1:9191"
	externalAddr2 := "127.0.0.1:9292"
	externalAddr3 := "127.0.0.1:9393"
	acceptCh1 := make(chan net.Conn)
	acceptCh2 := make(chan net.Conn)
	acceptCh3 := make(chan net.Conn)
	hijackStatus := "101 Switching Protocols"

	dialFunc := func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := net.Dial("tcp", addr)
		requireNoError(t, err)

		request := &http.Request{}
		request.URL, err = url.Parse("http://" + addr)
		requireNoError(t, err)

		requireNoError(t, request.Write(conn))
		resp, err := http.ReadResponse(bufio.NewReader(conn), request)
		requireNoError(t, err)
		requireEqual(t, hijackStatus, resp.Status)

		return conn, nil
	}

	newHandler := func(acceptCh chan net.Conn) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			hijacker, ok := w.(http.Hijacker)
			requireTrue(t, ok)

			conn, _, err := hijacker.Hijack()
			requireNoError(t, err)

			acceptCh <- conn
		}
	}

	// Start up three listeners.
	go http.ListenAndServe(externalAddr1, newHandler(acceptCh1))
	go http.ListenAndServe(externalAddr2, newHandler(acceptCh2))
	go http.ListenAndServe(externalAddr3, newHandler(acceptCh3))

	app1, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr1), app.WithExternalConn(dialFunc, acceptCh1))
	defer cleanup()

	app2, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr2), app.WithExternalConn(dialFunc, acceptCh2), app.WithCluster([]string{externalAddr1}))
	defer cleanup()

	requireNoError(t, app2.Ready(context.Background()))

	app3, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr3), app.WithExternalConn(dialFunc, acceptCh3), app.WithCluster([]string{externalAddr1}))
	defer cleanup()

	requireNoError(t, app3.Ready(context.Background()))

	// Get a client from the first node (likely the leader).
	cli, err := app1.Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	// Ensure entries exist for each cluster member.
	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)
	assertEqual(t, externalAddr1, cluster[0].Address)
	assertEqual(t, externalAddr2, cluster[1].Address)
	assertEqual(t, externalAddr3, cluster[2].Address)

	// Every cluster member should be a voter.
	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
}

// TestExternalPipe creates a 3-member cluster using net.Pipe
// and ensures the cluster is successfully created, and that the connection is
// handled manually.
func TestExternalConnWithPipe(t *testing.T) {
	externalAddr1 := "first"
	externalAddr2 := "second"
	externalAddr3 := "third"
	acceptCh1 := make(chan net.Conn)
	acceptCh2 := make(chan net.Conn)
	acceptCh3 := make(chan net.Conn)

	dialChannels := map[string]chan net.Conn{
		externalAddr1: acceptCh1,
		externalAddr2: acceptCh2,
		externalAddr3: acceptCh3,
	}

	dialFunc := func(_ context.Context, addr string) (net.Conn, error) {
		client, server := net.Pipe()

		dialChannels[addr] <- server

		return client, nil
	}

	app1, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr1), app.WithExternalConn(dialFunc, acceptCh1))
	defer cleanup()

	app2, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr2), app.WithExternalConn(dialFunc, acceptCh2), app.WithCluster([]string{externalAddr1}))
	defer cleanup()

	requireNoError(t, app2.Ready(context.Background()))

	app3, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr3), app.WithExternalConn(dialFunc, acceptCh3), app.WithCluster([]string{externalAddr1}))
	defer cleanup()

	requireNoError(t, app3.Ready(context.Background()))

	// Get a client from the first node (likely the leader).
	cli, err := app1.Leader(context.Background())
	requireNoError(t, err)
	defer cli.Close()

	// Ensure entries exist for each cluster member.
	cluster, err := cli.Cluster(context.Background())
	requireNoError(t, err)
	assertEqual(t, externalAddr1, cluster[0].Address)
	assertEqual(t, externalAddr2, cluster[1].Address)
	assertEqual(t, externalAddr3, cluster[2].Address)

	// Every cluster member should be a voter.
	assertEqual(t, client.Voter, cluster[0].Role)
	assertEqual(t, client.Voter, cluster[1].Role)
	assertEqual(t, client.Voter, cluster[2].Role)
}

func TestParallelNewApp(t *testing.T) {
	t.Parallel()
	for i := range 100 {
		i := i
		t.Run(fmt.Sprintf("run-%d", i), func(tt *testing.T) {
			tt.Parallel()
			// TODO: switch this to tt.TempDir when we switch to
			tmpDir := filepath.Join(os.TempDir(), strings.ReplaceAll(tt.Name(), "/", "-"))
			requireNoError(tt, os.MkdirAll(tmpDir, 0o700))
			dqApp, err := app.New(tmpDir,
				app.WithAddress(fmt.Sprintf("127.0.0.1:%d", 10200+i)),
			)
			requireNoError(tt, err)
			defer func() {
				_ = dqApp.Close()
				_ = os.RemoveAll(tmpDir)
			}()
		})
	}
}

func newAppWithDir(t *testing.T, dir string, options ...app.Option) (*app.App, func()) {
	t.Helper()

	appIndex++

	index := appIndex
	log := func(l client.LogLevel, format string, a ...any) {
		format = fmt.Sprintf("%s - %d: %s: %s", time.Now().Format("15:04:01.000"), index, l.String(), format)
		t.Logf(format, a...)
	}

	cert, pool := loadCert(t)
	options = append(options, app.WithLogFunc(log), app.WithTLS(app.SimpleTLSConfig(cert, pool)))

	app, err := app.New(dir, options...)
	requireNoError(t, err)

	cleanup := func() {
		requireNoError(t, app.Close())
	}

	return app, cleanup
}

func newAppWithNoTLS(t *testing.T, options ...app.Option) (*app.App, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	appIndex++

	index := appIndex
	log := func(l client.LogLevel, format string, a ...any) {
		format = fmt.Sprintf("%s - %d: %s: %s", time.Now().Format("15:04:01.000"), index, l.String(), format)
		t.Logf(format, a...)
	}

	options = append(options, app.WithLogFunc(log))

	app, err := app.New(dir, options...)
	requireNoError(t, err)

	cleanup := func() {
		requireNoError(t, app.Close())
		dirCleanup()
	}

	return app, cleanup
}

// Loads the test TLS certificates.
func loadCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	crt := filepath.Join("testdata", "cluster.crt")
	key := filepath.Join("testdata", "cluster.key")

	keypair, err := tls.LoadX509KeyPair(crt, key)
	requireNoError(t, err)

	data, err := os.ReadFile(crt)
	requireNoError(t, err)

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		t.Fatal("bad certificate")
	}

	return keypair, pool
}

var appIndex int

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "cowsql-app-test-")
	assertNoError(t, err)

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return dir, cleanup
}

func Test_TxRowsAffected(t *testing.T) {
	app, cleanup := newAppWithNoTLS(t, app.WithAddress("127.0.0.1:9001"))
	defer cleanup()

	err := app.Ready(context.Background())
	requireNoError(t, err)

	db, err := app.Open(context.Background(), "test")
	requireNoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `
CREATE TABLE test (
	id            TEXT PRIMARY KEY,
	value         INT
);`)
	requireNoError(t, err)

	// Insert watermark
	err = tx(context.Background(), db, func(ctx context.Context, tx *sql.Tx) error {
		query := `
INSERT INTO test
	(id, value)
VALUES
	('id0', -1);
	`
		result, err := tx.ExecContext(ctx, query)
		if err != nil {
			return err
		}
		_, err = result.RowsAffected()
		if err != nil {
			return err
		}
		return nil
	})
	requireNoError(t, err)

	// Update watermark
	err = tx(context.Background(), db, func(ctx context.Context, tx *sql.Tx) error {
		query := `
UPDATE test
SET
	value = 1
WHERE id = 'id0';
	`
		result, err := tx.ExecContext(ctx, query)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("expected 1 row affected, got %d", affected)
		}
		return nil
	})
	requireNoError(t, err)
}

func tx(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}
