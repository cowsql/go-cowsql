package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/cowsql/go-cowsql/cluster/internal/util/file"
	"github.com/cowsql/go-cowsql/cluster/internal/util/tcp"
)

type cowsqlProxyError struct {
	first  error
	second error
}

func (e cowsqlProxyError) Error() string {
	msg := ""
	if e.first != nil {
		msg += "first: " + e.first.Error()
	}

	if e.second != nil {
		if e.first != nil {
			msg += " "
		}

		msg += "second: " + e.second.Error()
	}

	return msg
}

// Copy incoming TLS streams from upgraded HTTPS connections into Unix sockets
// connected to the cowsql task.
func runCowsqlProxy(stopCh chan struct{}, bindAddress string, acceptCh chan net.Conn) {
	for {
		remote := <-acceptCh
		local, err := net.Dial("unix", bindAddress)
		if err != nil {
			continue
		}

		go cowsqlProxy("dqlite", stopCh, remote, local)
	}
}

// Copies data between a remote TLS network connection and a local unix socket.
// Accepts name argument that can be used to identify the connection in the logs.
func cowsqlProxy(name string, stopCh chan struct{}, remote net.Conn, local net.Conn) {
	l := slog.With("name", name, "local", remote.LocalAddr(), "remote", remote.RemoteAddr())
	l.Debug("Cowsql proxy started")
	defer l.Debug("Cowsql proxy stopped")

	remoteTCP, err := tcp.ExtractConn(remote)
	if err != nil {
		l.Warn("Failed extracting TCP connection from remote connection", "err", err)
	} else {
		err := tcp.SetTimeouts(remoteTCP, time.Second*30)
		if err != nil {
			l.Warn("Failed setting TCP timeouts on remote connection", "err", err)
		}
	}

	remoteToLocal := make(chan error)
	localToRemote := make(chan error)

	// Start copying data back and forth until either the client or the
	// server get closed or hit an error.
	go func() {
		_, err := file.SafeCopy(local, remote)
		remoteToLocal <- err
	}()

	go func() {
		_, err := file.SafeCopy(remote, local)
		localToRemote <- err
	}()

	errs := make([]error, 2)

	select {
	case <-stopCh:
		// Force closing, ignore errors.
		_ = remote.Close()
		_ = local.Close()
		<-remoteToLocal
		<-localToRemote
	case err := <-remoteToLocal:
		if err != nil {
			errs[0] = fmt.Errorf("remote -> local: %w", err)
		}

		_ = local.(*net.UnixConn).CloseRead()
		err = <-localToRemote
		if err != nil {
			errs[1] = fmt.Errorf("local -> remote: %w", err)
		}

		_ = remote.Close()
		_ = local.Close()
	case err := <-localToRemote:
		if err != nil {
			errs[0] = fmt.Errorf("local -> remote: %w", err)
		}

		_ = remoteTCP.CloseRead()
		err = <-remoteToLocal
		if err != nil {
			errs[1] = fmt.Errorf("remote -> local: %w", err)
		}

		_ = local.Close()
		_ = remote.Close()
	}

	if errs[0] != nil || errs[1] != nil {
		err := cowsqlProxyError{first: errs[0], second: errs[1]}
		l.Debug("Cowsql proxy failed", "err", err)
	}
}
