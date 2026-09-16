package db

import (
	"github.com/cowsql/go-cowsql/client"
)

// RaftNode holds information about a single node in the cowsql raft cluster.
//
// This is just a convenience alias for the equivalent data structure in the
// cowsql client package.
type RaftNode struct {
	ID      uint64          `json:"id"`
	Address string          `json:"address"`
	Role    client.NodeRole `json:"role"`

	Name string `json:"name"`
}

// RaftRole captures the role of cowsql/raft node.
type RaftRole = client.NodeRole

// RaftNode roles.
const (
	RaftVoter   = client.Voter
	RaftStandBy = client.StandBy
	RaftSpare   = client.Spare
)

// DefaultRaftNode represents a fully uninitialized raft node entry with ID: 1 and Address: 1, signifying an uninitialized system.
func DefaultRaftNode() *RaftNode {
	return &RaftNode{ID: 1, Address: "1", Name: ""}
}
