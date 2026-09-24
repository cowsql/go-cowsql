package db

import "time"

// NodeInfo holds information about a single member in a cluster.
type NodeInfo struct {
	ID            int64             // Stable node identifier
	Name          string            // User-assigned name of the node
	Address       string            // Network address of the node
	Description   string            // Node description (optional)
	Schema        int               // Schema version of the daemon running the member
	APIExtensions int               // Number of API extensions of the daemon running the member
	Heartbeat     time.Time         // Timestamp of the last heartbeat
	Roles         []string          // List of cluster roles
	Architecture  int               // Node architecture
	State         int               // Node state
	Config        map[string]string // Configuration for the node
	Groups        []string          // Cluster groups

	// heartbeatRefTime is the time at which the node row was read from the
	// database. It is used as the reference time for IsOffline so that a
	// long-running transaction (during which the heartbeat snapshot cannot be
	// refreshed) doesn't incorrectly classify members as offline just because
	// the transaction has been open longer than the offline threshold.
	heartbeatRefTime time.Time
}

// IsOffline returns true if the last successful heartbeat time of the node is
// older than the given threshold.
//
// The check uses the time at which this NodeInfo was read from the database as
// the reference point (if known), rather than time.Now(). This avoids false
// positives when the caller is operating inside a long-running transaction
// where the heartbeat snapshot can't be refreshed.
func (n NodeInfo) IsOffline(threshold time.Duration) bool {
	return nodeIsOffline(threshold, n.Heartbeat, n.heartbeatRefTime)
}

// SetHeartbeatRefTime sets the reference time used for IsOffline.
func (n *NodeInfo) SetHeartbeatRefTime(t time.Time) {
	n.heartbeatRefTime = t
}

// nodeIsOffline reports whether heartbeat is older than threshold relative to
// refTime. If refTime is the zero value, time.Now() is used as the reference.
func nodeIsOffline(threshold time.Duration, heartbeat time.Time, refTime time.Time) bool {
	if refTime.IsZero() {
		refTime = time.Now()
	}

	offlineTime := refTime.UTC().Add(-threshold)

	return heartbeat.Before(offlineTime) || heartbeat.Equal(offlineTime)
}

// HeartbeatMember contains specific cluster node info.
type HeartbeatMember struct {
	ID            int64     `json:"ID"`            //nolint:tagliatelle // ID field value in nodes table.
	Address       string    `json:"Address"`       //nolint:tagliatelle // Host and Port of node.
	Name          string    `json:"Name"`          //nolint:tagliatelle // Name of cluster member.
	RaftID        uint64    `json:"RaftID"`        //nolint:tagliatelle // ID field value in raft_nodes table, zero if non-raft node.
	RaftRole      int       `json:"RaftRole"`      //nolint:tagliatelle // Node role in the raft cluster, from the raft_nodes table
	LastHeartbeat time.Time `json:"LastHeartbeat"` //nolint:tagliatelle // Last time we received a successful response from node.
	Online        bool      `json:"Online"`        //nolint:tagliatelle // Calculated from offline threshold and LastHeatbeat time.
	Roles         []string  `json:"Roles"`         //nolint:tagliatelle // Supplementary non-database roles the member has.
	Updated       bool      `json:"Updated"`       //nolint:tagliatelle // Has node been updated during this heartbeat run.
}
