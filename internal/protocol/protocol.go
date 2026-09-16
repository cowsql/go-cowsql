package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Protocol sends and receive the cowsql message on the wire.
type Protocol struct {
	version uint64        // Protocol version
	conn    net.Conn      // Underlying network connection.
	closeCh chan struct{} // Stops the heartbeat when the connection gets closed
	mu      sync.Mutex    // Serialize requests
	netErr  error         // A network error occurred
}

func newProtocol(version uint64, conn net.Conn) *Protocol {
	protocol := &Protocol{
		version: version,
		conn:    conn,
		closeCh: make(chan struct{}),
	}

	return protocol
}

// Call invokes a cowsql RPC, sending a request message and receiving a
// response message.
func (p *Protocol) Call(ctx context.Context, request, response *Message) (err error) {
	// We need to take a lock since the cowsql server currently does not
	// support concurrent requests.
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.netErr != nil {
		return p.netErr
	}

	defer func() {
		if err == nil {
			return
		}

		var netErr *net.OpError
		if errors.As(err, &netErr) && netErr != nil {
			p.netErr = netErr
		}
	}()

	var budget time.Duration

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		err := p.conn.SetDeadline(deadline)
		if err != nil {
			return fmt.Errorf("failed to set connection deadline: %w", err)
		}

		budget = time.Until(deadline)

		defer func() {
			err := p.conn.SetDeadline(time.Time{})
			if err != nil {
				slog.Error("Failed to set connection deadline", "error", err)
			}
		}()
	}

	desc := requestDesc(request.mtype)

	err = p.send(request)
	if err != nil {
		return fmt.Errorf("call %s (budget %s): send: %w", desc, budget, err)
	}

	err = p.recv(response)
	if err != nil {
		return fmt.Errorf("call %s (budget %s): receive: %w", desc, budget, err)
	}

	return err
}

// More is used when a request maps to multiple responses.
func (p *Protocol) More(ctx context.Context, response *Message) error {
	return p.recv(response)
}

// Interrupt sends an interrupt request and awaits for the server's empty
// response.
func (p *Protocol) Interrupt(ctx context.Context, request *Message, response *Message) error {
	// We need to take a lock since the cowsql server currently does not
	// support concurrent requests.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		err := p.conn.SetDeadline(deadline)
		if err != nil {
			return fmt.Errorf("failed to set connection deadline: %w", err)
		}
		defer func() {
			err := p.conn.SetDeadline(time.Time{})
			if err != nil {
				slog.Error("Failed to set connection deadline", "error", err)
			}
		}()
	}

	EncodeInterrupt(request, 0)

	err := p.send(request)
	if err != nil {
		return fmt.Errorf("failed to send interrupt request: %w", err)
	}

	for {
		err := p.recv(response)
		if err != nil {
			return fmt.Errorf("failed to receive response: %w", err)
		}

		mtype, _ := response.getHeader()

		if mtype == ResponseEmpty {
			break
		}
	}

	return nil
}

// Close the client connection.
func (p *Protocol) Close() error {
	close(p.closeCh)

	return p.conn.Close()
}

func (p *Protocol) send(req *Message) error {
	err := p.sendHeader(req)
	if err != nil {
		return fmt.Errorf("header: %w", err)
	}

	err = p.sendBody(req)
	if err != nil {
		return fmt.Errorf("body: %w", err)
	}

	return nil
}

func (p *Protocol) sendHeader(req *Message) error {
	n, err := p.conn.Write(req.header)
	if err != nil {
		return err
	}

	if n != messageHeaderSize {
		return io.ErrShortWrite
	}

	return nil
}

func (p *Protocol) sendBody(req *Message) error {
	buf := req.body.Bytes[:req.body.Offset]

	n, err := p.conn.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func (p *Protocol) recv(res *Message) error {
	res.reset()

	err := p.recvHeader(res)
	if err != nil {
		return fmt.Errorf("header: %w", err)
	}

	err = p.recvBody(res)
	if err != nil {
		return fmt.Errorf("body: %w", err)
	}

	return nil
}

func (p *Protocol) recvHeader(res *Message) error {
	err := p.recvPeek(res.header)
	if err != nil {
		return err
	}

	res.words = binary.LittleEndian.Uint32(res.header[0:])
	res.mtype = res.header[4]
	res.schema = res.header[5]
	res.extra = binary.LittleEndian.Uint16(res.header[6:])

	return nil
}

func (p *Protocol) recvBody(res *Message) error {
	n := int(res.words) * messageWordSize

	for n > len(res.body.Bytes) {
		// Grow message buffer.
		bytes := make([]byte, len(res.body.Bytes)*2)
		res.body.Bytes = bytes
	}

	buf := res.body.Bytes[:n]

	err := p.recvPeek(buf)
	if err != nil {
		return err
	}

	return nil
}

// Read until buf is full.
func (p *Protocol) recvPeek(buf []byte) error {
	for offset := 0; offset < len(buf); {
		n, err := p.recvFill(buf[offset:])
		if err != nil {
			return err
		}

		offset += n
	}

	return nil
}

// Try to fill buf, but perform at most one read.
func (p *Protocol) recvFill(buf []byte) (int, error) {
	// Read new data: try a limited number of times.
	//
	// This technique is copied from bufio.Reader.
	for i := messageMaxConsecutiveEmptyReads; i > 0; i-- {
		n, err := p.conn.Read(buf)
		if n < 0 {
			panic(errNegativeRead)
		}

		if err != nil {
			return -1, err
		}

		if n > 0 {
			return n, nil
		}
	}

	return -1, io.ErrNoProgress
}

/*
func (p *Protocol) heartbeat() {
	request := Message{}
	request.Init(16)
	response := Message{}
	response.Init(512)

	for {
		delay := c.heartbeatTimeout / 3

		//c.logger.Debug("sending heartbeat", zap.Duration("delay", delay))
		time.Sleep(delay)

		// Check if we've been closed.
		select {
		case <-c.closeCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		EncodeHeartbeat(&request, uint64(time.Now().Unix()))

		err := c.Call(ctx, &request, &response)

		// We bail out upon failures.
		//
		// TODO: make the client survive temporary disconnections.
		if err != nil {
			cancel()
			//c.logger.Error("heartbeat failed", zap.Error(err))
			return
		}

		//addresses, err := DecodeNodes(&response)
		_, err = DecodeNodes(&response)
		if err != nil {
			cancel()
			//c.logger.Error("invalid heartbeat response", zap.Error(err))
			return
		}

		// if err := c.store.Set(ctx, addresses); err != nil {
		// 	cancel()
		// 	c.logger.Error("failed to update servers", zap.Error(err))
		// 	return
		// }

		cancel()

		request.Reset()
		response.Reset()
	}
}
*/

// DecodeNodeCompat handles also pre-1.0 legacy server messages.
func DecodeNodeCompat(protocol *Protocol, response *Message) (uint64, string, error) {
	if protocol.version == VersionLegacy {
		address, err := DecodeNodeLegacy(response)
		if err != nil {
			return 0, "", err
		}

		return 0, address, nil
	}

	return DecodeNode(response)
}
