//go:build !nosqlite3

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"

	"github.com/cowsql/go-cowsql/cluster/example"
)

func main() {
	var (
		dirFlag     string
		addressFlag string
		targetFlag  string
		nameFlag    string
	)

	root := &cobra.Command{
		Use:   "cluster",
		Short: "COWSQL Cluster example",
	}

	server := &cobra.Command{
		Use:   "daemon",
		Short: "COWSQL Cluster example daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			return daemon(cmd.Context(), addressFlag, nameFlag, dirFlag)
		},
	}

	client := &cobra.Command{
		Use:   "client",
		Short: "COWSQL Cluster example client",
	}

	bootstrap := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap the first cluster member",
		RunE: func(cmd *cobra.Command, args []string) error {
			return api(cmd.Context(), bootstrap, addressFlag, "")
		},
	}

	join := &cobra.Command{
		Use:   "join",
		Short: "Join an existing cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			return api(cmd.Context(), join, addressFlag, targetFlag)
		},
	}

	list := &cobra.Command{
		Use:   "list",
		Short: "List cluster members",
		RunE: func(cmd *cobra.Command, args []string) error {
			return api(cmd.Context(), list, addressFlag, "")
		},
	}

	client.AddCommand(list)
	client.AddCommand(join)
	client.AddCommand(bootstrap)
	root.AddCommand(client)
	root.AddCommand(server)

	serverFlags := server.Flags()
	serverFlags.StringVarP(&addressFlag, "address", "a", "", "address:port used by the server")
	serverFlags.StringVarP(&dirFlag, "dir", "d", "", "absolute path under which to persist cluster data")
	serverFlags.StringVarP(&nameFlag, "name", "n", "", "identifying name of the cluster member")

	list.Flags().AddFlag(server.Flag("address"))

	bootstrap.Flags().AddFlag(server.Flag("address"))

	joinFlags := join.Flags()
	joinFlags.AddFlag(server.Flag("address"))
	joinFlags.StringVarP(&targetFlag, "target", "t", "", "address:port of an existing cluster member to request joining")

	root.CompletionOptions = cobra.CompletionOptions{HiddenDefaultCmd: true}

	err := root.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func daemon(ctx context.Context, address string, serverName string, dir string) error {
	ctx, cancel := signal.NotifyContext(ctx, unix.SIGPWR, unix.SIGTERM, unix.SIGINT, unix.SIGQUIT)
	defer cancel()

	daemon := example.NewDaemon(address, serverName, dir)

	return daemon.Start(ctx)
}

type action int

const (
	bootstrap action = iota
	join
	list
)

func api(ctx context.Context, apiAction action, address string, target string) error {
	method := http.MethodPost

	var (
		apiPath string
		body    io.Reader
	)

	switch apiAction {
	case bootstrap:
		apiPath = "/bootstrap"
	case join:
		apiPath = "/join"

		b, err := json.Marshal(example.JoinPost{ClusterAddress: target})
		if err != nil {
			return err
		}

		body = bytes.NewReader(b)
	case list:
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(ctx, method, "https://"+address+apiPath, body)
	if err != nil {
		return err
	}

	c := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec
			},
		},
	}

	resp, err := c.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Unexpected status: %d", resp.StatusCode)
	}

	if apiAction == list {
		var data []example.ListGet

		err := json.NewDecoder(resp.Body).Decode(&data)
		if err != nil {
			return err
		}

		for _, d := range data {
			fmt.Printf("Name: %q  Address: %q  Role: %q  Offline: %v  Heartbeat: %q\n", d.Name, d.Address, d.Role, d.Offline, d.LastHeartbeat) //nolint:forbidigo
		}
	}

	return nil
}
