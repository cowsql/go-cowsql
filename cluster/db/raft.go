package db

import "github.com/cowsql/go-cowsql/client"

// RaftNode holds information about a single node in the cowsql raft cluster.
//
// This is just a convenience alias for the equivalent data structure in the
// cowsql client package.
type RaftNode struct {
	client.NodeInfo
	Name string
}

// RaftRole captures the role of cowsql/raft node.
type RaftRole = client.NodeRole

// RaftNode roles.
const (
	RaftVoter   = client.Voter
	RaftStandBy = client.StandBy
	RaftSpare   = client.Spare
)
