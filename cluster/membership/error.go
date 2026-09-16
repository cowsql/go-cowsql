package membership

import (
	"errors"
	"strings"
)

var (
	// errClusterBusy is returned by cowsql if attempting a configuration change while another one is in progress.
	// This error tells us we can retry and probably succeed or fail due to something else.
	// The error code here is SQLITE_BUSY. As cowsql only exposes the error as text, matching must be done on the error string.
	errClusterBusy = errors.New("a configuration change is already in progress (5)")

	// ErrNoOnlineVoter indicates that no online voter was found to transfer leadership to.
	ErrNoOnlineVoter = errors.New("No online voter found")

	// ErrNodeIsNotClustered indicates the node is not clustered.
	ErrNodeIsNotClustered = errors.New("Server is not clustered")

	// ErrNotLeader signals that a node not the leader.
	ErrNotLeader = errors.New("Not leader")
)

// IsClusterBusyErr returns whether an error contains the errClusterBusy error message.
func IsClusterBusyErr(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), errClusterBusy.Error())
}
