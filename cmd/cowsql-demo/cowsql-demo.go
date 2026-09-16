package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/cowsql/go-cowsql/app"
	"github.com/cowsql/go-cowsql/client"
)

func main() {
	var (
		api     string
		db      string
		join    *[]string
		dir     string
		verbose bool
		crt     string
		key     string
	)

	cmd := &cobra.Command{
		Use:   "cowsql-demo",
		Short: "Demo application using cowsql",
		Long: `This demo shows how to integrate a Go application with cowsql.

Complete documentation is available at https://github.com/cowsql/go-cowsql`,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := filepath.Join(dir, db)

			err := os.MkdirAll(dir, 0o755)
			if err != nil {
				return fmt.Errorf("can't create %s: %w", dir, err)
			}

			logFunc := func(l client.LogLevel, format string, a ...any) {
				if !verbose {
					return
				}

				log.Printf(fmt.Sprintf("%s: %s: %s\n", api, l.String(), format), a...)
			}

			options := []app.Option{app.WithAddress(db), app.WithCluster(*join), app.WithLogFunc(logFunc)}

			// Set TLS options
			if (crt != "" && key == "") || (key != "" && crt == "") {
				return errors.New("both TLS certificate and key must be given")
			}

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

				options = append(options, app.WithTLS(app.SimpleTLSConfig(cert, pool)))
			}

			app, err := app.New(dir, options...)
			if err != nil {
				return err
			}

			err = app.Ready(context.Background())
			if err != nil {
				return err
			}

			db, err := app.Open(context.Background(), "demo")
			if err != nil {
				return err
			}

			_, err = db.Exec(schema)
			if err != nil {
				return err
			}

			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimLeft(r.URL.Path, "/")
				result := ""

				switch r.Method {
				case http.MethodGet:
					row := db.QueryRow(query, key)

					err := row.Scan(&result)
					if err != nil {
						result = "Error: " + err.Error()
					}

				case http.MethodPut:
					result = "done"
					value, _ := io.ReadAll(r.Body)

					_, err := db.Exec(update, key, string(value))
					if err != nil {
						result = "Error: " + err.Error()
					}
				default:
					result = fmt.Sprintf("Error: unsupported method %q", r.Method)
				}

				_, _ = fmt.Fprintf(w, "%s\n", result)
			})

			var lc net.ListenConfig

			listener, err := lc.Listen(context.TODO(), "tcp", api)
			if err != nil {
				return err
			}

			go func() { _ = http.Serve(listener, nil) }() //nolint:gosec

			ch := make(chan os.Signal, 32)
			signal.Notify(ch, syscall.SIGPWR)
			signal.Notify(ch, syscall.SIGINT)
			signal.Notify(ch, syscall.SIGQUIT)
			signal.Notify(ch, syscall.SIGTERM)

			<-ch

			_ = listener.Close()
			_ = db.Close()

			_ = app.Handover(context.Background())
			_ = app.Close()

			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&api, "api", "a", "", "address used to expose the demo API")
	flags.StringVarP(&db, "db", "d", "", "address used for internal database replication")
	join = flags.StringSliceP("join", "j", nil, "database addresses of existing nodes")
	flags.StringVarP(&dir, "dir", "D", "/tmp/cowsql-demo", "data directory")
	flags.BoolVarP(&verbose, "verbose", "v", false, "verbose logging")
	flags.StringVarP(&crt, "cert", "c", "", "public TLS cert")
	flags.StringVarP(&key, "key", "k", "", "private TLS key")

	err := cmd.MarkFlagRequired("api")
	if err != nil {
		os.Exit(1)
	}

	err = cmd.MarkFlagRequired("db")
	if err != nil {
		os.Exit(1)
	}

	err = cmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

const (
	schema = "CREATE TABLE IF NOT EXISTS model (key TEXT, value TEXT, UNIQUE(key))"
	query  = "SELECT value FROM model WHERE key = ?"
	update = "INSERT OR REPLACE INTO model(key, value) VALUES(?, ?)"
)
