package client

import (
	"github.com/cowsql/go-cowsql/internal/protocol"
)

func (c *Client) Protocol() *protocol.Protocol {
	return c.protocol
}
