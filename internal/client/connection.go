package client

import (
	"bufio"
	"encoding/binary"
	"godel/internal/protocol"
	"io"
	"net"
	"sync"
)

type listener struct {
	correlationID int32
	callback      func(*protocol.BaseResponse)
	oneShot       bool
}

type outgoingRequest struct {
	req    *protocol.BaseRequest
	result chan error
}

type Connection struct {
	conn      net.Conn
	writer    *bufio.Writer
	reader    *bufio.Reader
	listeners []listener
	mu        sync.Mutex
	onError   func(*Connection, error)

	closeCh  chan struct{}
	requests chan outgoingRequest
}

// ConnectToBroker connects to a TCP server and returns a connection handle.
func ConnectToBroker(addr string, onError func(*Connection, error)) (*Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		listeners: []listener{},
		conn:      conn,
		onError:   onError,

		requests: make(chan outgoingRequest, 100),
		closeCh:  make(chan struct{}),
	}

	c.startListening()
	c.startSending()

	return c, nil
}

// startListening reads messages from the server and dispatches them to listeners.
func (c *Connection) startListening() {
	go func() {
		for {
			select {
			case <-c.closeCh:
				return
			default:
				// read length prefix
				// var length int32
				// if err := binary.Read(c.reader, binary.BigEndian, &length); err != nil {
				// 	if err != io.EOF {
				// 		c.onError(c, err)
				// 	}
				// 	return
				// }

				// data := make([]byte, length)
				// if _, err := io.ReadFull(c.reader, data); err != nil {
				// 	c.onError(c, err)
				// 	return
				// }

				resp, err := protocol.DeserializeResponse(c.reader)
				if err != nil {
					c.onError(c, err)
					continue
				}

				// dispatch to listeners
				c.mu.Lock()
				for i := 0; i < len(c.listeners); i++ {
					l := c.listeners[i]
					if l.correlationID == resp.CorrelationID {
						// run callback in a separate goroutine
						go func(cb func(*protocol.BaseResponse), r *protocol.BaseResponse) {
							cb(r)
						}(l.callback, resp)

						// remove one-shot listener
						if l.oneShot {
							c.listeners = append(c.listeners[:i], c.listeners[i+1:]...)
							i--
						}
					}
				}
				c.mu.Unlock()
			}
		}
	}()
}

// startSending writes queued requests to the server.
func (c *Connection) startSending() {
	go func() {
		for {
			select {
			case <-c.closeCh:
				return
			case out := <-c.requests:
				data := out.req.Serialize()
				// if err != nil {
				// 	out.result <- err
				// 	continue
				// }

				if err := binary.Write(c.writer, binary.BigEndian, int32(len(data))); err != nil {
					out.result <- err
					c.onError(c, err)
					return
				}

				if _, err := c.writer.Write(data); err != nil {
					out.result <- err
					c.onError(c, err)
					return
				}

				if err := c.writer.Flush(); err != nil {
					out.result <- err
					c.onError(c, err)
					return
				}

				// success
				out.result <- nil
			}
		}
	}()
}

// SendMessage sends a request to the server and waits until it is actually written.
// Returns an error if sending fails or connection is closed.
func (c *Connection) SendMessage(req *protocol.BaseRequest) error {
	resultCh := make(chan error, 1)

	select {
	case c.requests <- outgoingRequest{req: req, result: resultCh}:
	case <-c.closeCh:
		return io.ErrClosedPipe
	}

	return <-resultCh
}

// AppendListener adds a response listener.
// If oneShot is true, it will be removed after the first matching response.
func (c *Connection) AppendListener(correlationID int32, callback func(*protocol.BaseResponse), oneShot bool) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listeners = append(c.listeners, listener{
		correlationID: correlationID,
		callback:      callback,
		oneShot:       oneShot,
	})

	return func() {
		c.removeListener(correlationID)
	}
}

// Close shuts down the connection and all goroutines.
func (c *Connection) Close() {
	close(c.closeCh)
	_ = c.conn.Close()
}

// RemoveListener removes all listeners matching the given correlationID.
func (c *Connection) removeListener(correlationID int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newListeners := c.listeners[:0] // reuse underlying array
	for _, l := range c.listeners {
		if l.correlationID != correlationID {
			newListeners = append(newListeners, l)
		}
	}
	c.listeners = newListeners
}
