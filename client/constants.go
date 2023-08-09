package client

import (
	"github.com/cowsql/go-cowsql/internal/protocol"
)

// Node roles
const (
	Voter   = protocol.Voter
	StandBy = protocol.StandBy
	Spare   = protocol.Spare
)
