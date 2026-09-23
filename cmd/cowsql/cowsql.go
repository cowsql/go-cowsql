package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"

	"github.com/cowsql/go-cowsql/app"
	"github.com/cowsql/go-cowsql/client"
	"github.com/cowsql/go-cowsql/internal/shell"
)

func main() {
	var (
		crt     string
		key     string
		servers *[]string
		format  string
	)

	cmd := &cobra.Command{
		Use:   "cowsql -s <servers> <database> [command]",
		Short: "Standard cowsql shell",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(*servers) == 0 {
				return errors.New("no servers provided")
			}

			var (
				store client.NodeStore
				err   error
			)

			first := (*servers)[0]
			if strings.HasPrefix(first, "file://") {
				if len(*servers) > 1 {
					return errors.New("can't mix server store and explicit list")
				}

				path := first[len("file://"):]

				_, err := os.Stat(path)
				if err != nil {
					return fmt.Errorf("open servers store: %w", err)
				}

				store, err = client.DefaultNodeStore(path)
				if err != nil {
					return fmt.Errorf("open servers store: %w", err)
				}
			} else {
				infos := make([]client.NodeInfo, len(*servers))
				for i, address := range *servers {
					infos[i].Address = address
				}

				store = client.NewInmemNodeStore()
				_ = store.Set(context.Background(), infos)
			}

			if (crt != "" && key == "") || (key != "" && crt == "") {
				return errors.New("both TLS certificate and key must be given")
			}

			dial := client.DefaultDialFunc

			if crt != "" {
				cert, err := tls.LoadX509KeyPair(crt, key)
				if err != nil {
					return err
				}

				data, err := os.ReadFile(crt)
				if err != nil {
					return err
				}

				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(data) {
					return errors.New("bad certificate")
				}

				config := app.SimpleDialTLSConfig(cert, pool)
				dial = client.DialFuncWithTLS(dial, config)
			}

			sh, err := shell.New(args[0], store, shell.WithDialFunc(dial), shell.WithFormat(format))
			if err != nil {
				return err
			}

			if len(args) > 1 {
				for input := range strings.SplitSeq(args[1], ";") {
					result, err := sh.Process(context.Background(), input)
					if err != nil {
						return err
					} else if result != "" {
						fmt.Println(result) //nolint:forbidigo
					}
				}

				return nil
			}

			line := liner.NewLiner()
			defer line.Close()

			for {
				input, err := line.Prompt("cowsql> ")
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				result, err := sh.Process(context.Background(), input)
				if err != nil {
					fmt.Println("Error: ", err) //nolint:forbidigo
				} else {
					line.AppendHistory(input)

					if result != "" {
						fmt.Println(result) //nolint:forbidigo
					}
				}
			}

			return nil
		},
	}

	flags := cmd.Flags()
	servers = flags.StringSliceP("servers", "s", nil, "comma-separated list of db servers, or file://<store>")
	flags.StringVarP(&crt, "cert", "c", "", "public TLS cert")
	flags.StringVarP(&key, "key", "k", "", "private TLS key")
	flags.StringVarP(&format, "format", "f", "tabular", "output format (tabular, json)")

	err := cmd.MarkFlagRequired("servers")
	if err != nil {
		os.Exit(1)
	}

	err = cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
