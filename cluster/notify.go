package cluster

import (
	"context"

	"github.com/cowsql/go-cowsql/cluster/tls"
)

// Notifier is a function that invokes the given function against each node in
// the cluster excluding the invoking one.
type Notifier func(hook func(ctx context.Context, address string, networkCert, serverCert tls.CertInfo) error) []error

// NotifierPolicy can be used to tweak the behavior of NewNotifier in case of
// some nodes are down.
type NotifierPolicy int

// Possible notification policies.
const (
	NotifyAll    NotifierPolicy = iota // Requires that all nodes are up.
	NotifyAlive                        // Only notifies nodes that are alive
	NotifyTryAll                       // Attempt to notify all nodes regardless of state.
)
