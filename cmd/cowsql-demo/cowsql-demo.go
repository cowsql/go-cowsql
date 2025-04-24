package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/cowsql/go-cowsql/app"
	"github.com/cowsql/go-cowsql/client"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

func main() {
	var api string
	var db string
	var join *[]string
	var dir string
	var verbose bool
	var crt string
	var key string

	cmd := &cobra.Command{
		Use:   "cowsql-demo",
		Short: "Demo application using cowsql",
		Long: `This demo shows how to integrate a Go application with cowsql.

Complete documentation is available at https://github.com/cowsql/go-cowsql`,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := filepath.Join(dir, db)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return errors.Wrapf(err, "can't create %s", dir)
			}
			logFunc := func(l client.LogLevel, format string, a ...interface{}) {
				if !verbose {
					return
				}
				log.Printf(fmt.Sprintf("%s: %s: %s\n", api, l.String(), format), a...)
			}

			options := []app.Option{app.WithAddress(db), app.WithCluster(*join), app.WithLogFunc(logFunc)}

			// Set TLS options
			if (crt != "" && key == "") || (key != "" && crt == "") {
				return fmt.Errorf("both TLS certificate and key must be given")
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
					return fmt.Errorf("bad certificate")
				}
				options = append(options, app.WithTLS(app.SimpleTLSConfig(cert, pool)))
			}

			app, err := app.New(dir, options...)
			if err != nil {
				return err
			}

			if err := app.Ready(context.Background()); err != nil {
				return err
			}

			db, err := app.Open(context.Background(), "demo")
			if err != nil {
				return err
			}

			if _, err := db.Exec(schema); err != nil {
				return err
			}

			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimLeft(r.URL.Path, "/")
				result := ""
				switch r.Method {
				case "GET":
					row := db.QueryRow(query, key)
					if err := row.Scan(&result); err != nil {
						result = fmt.Sprintf("Error: %s", err.Error())
					}
					break
				case "PUT":
					result = "done"
					value, _ := io.ReadAll(r.Body)
					if _, err := db.Exec(update, key, string(value[:])); err != nil {
						result = fmt.Sprintf("Error: %s", err.Error())
					}
				default:
					result = fmt.Sprintf("Error: unsupported method %q", r.Method)

				}
				fmt.Fprintf(w, "%s\n", result)
			})

			listener, err := net.Listen("tcp", api)
			if err != nil {
				return err
			}

			go http.Serve(listener, nil)

			ch := make(chan os.Signal, 32)
			signal.Notify(ch, unix.SIGPWR)
			signal.Notify(ch, unix.SIGINT)
			signal.Notify(ch, unix.SIGQUIT)
			signal.Notify(ch, unix.SIGTERM)

			<-ch

			listener.Close()
			db.Close()

			app.Handover(context.Background())
			app.Close()

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

	cmd.MarkFlagRequired("api")
	cmd.MarkFlagRequired("db")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

const (
	schema = "CREATE TABLE IF NOT EXISTS model (key TEXT, value TEXT, UNIQUE(key))"
	query  = "SELECT value FROM model WHERE key = ?"
	update = "INSERT OR REPLACE INTO model(key, value) VALUES(?, ?)"
)
