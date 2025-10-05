package client

import (
	"bufio"
	"errors"
	"godel/internal/protocol"
	"io"
	"net"
)

var ErrCloseConnection = errors.New("close.connection")

type listener struct {
	correlationID int32
	callback      func(*protocol.BaseResponse) error
}

type Connection struct {
	conn      net.Conn
	writer    *bufio.Writer
	reader    *bufio.Reader
	listeners []listener
	onError   func(*Connection, error)

	closeCh  chan struct{}
	requests chan *protocol.BaseRequest
}

func ConnectToBroker(addr string, onError func(*Connection, error)) (*Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	c := &Connection{
		reader:    reader,
		writer:    writer,
		listeners: []listener{},
		conn:      conn,
		onError:   onError,

		requests: make(chan *protocol.BaseRequest, 100),
		closeCh:  make(chan struct{}),
	}

	c.startListening()
	c.startSending()

	return c, nil
}

func (c *Connection) startListening() {
	go func() {
		defer c.Close() // ensure cleanup
		for {
			msg, err := protocol.DeserializeResponse(c.reader)
			if err != nil {
				if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
					// normal disconnect
					return
				}
				c.onError(c, err)
				return
			}

			for i := range c.listeners {
				if c.listeners[i].correlationID == msg.CorrelationID {
					err = c.listeners[i].callback(msg)
					if err != nil {
						if err == ErrCloseConnection {
							c.onError(c, err)
							return
						}
						c.onError(c, err)
					}
				}
			}
		}
	}()
}

// func (c *Connection) startListening() {
// 	go func() {
// 		for {
// 			msg, err := protocol.DeserializeResponse(c.reader)
// 			if err != nil {
// 				c.onError(c, err)
// 				return
// 			}

// 			for i := range c.listeners {
// 				if c.listeners[i].correlationID == msg.CorrelationID {
// 					err = c.listeners[i].callback(msg)
// 					if err != nil && c != nil {
// 						c.onError(c, err)
// 						continue
// 					}

// 					if err == ErrCloseConnection {
// 						c.onError(c, err)
// 						c.Close()
// 						return
// 					}
// 				}
// 			}
// 		}
// 	}()
// }

func (c *Connection) startSending() {
	go func() {
		for {
			select {
			case <-c.closeCh:
				return
			case m := <-c.requests:
				ser := m.Serialize()

				_, err := c.writer.Write(ser)
				if err != nil {
					c.onError(c, err)
					continue
				}

				err = c.writer.Flush()
				if err != nil {
					c.onError(c, err)
					continue
				}
			}
		}
	}()
}

func (c *Connection) SendMessage(m *protocol.BaseRequest) error {
	c.requests <- m
	return nil
}

func (c *Connection) AppendListener(corrID int32, cb func(r *protocol.BaseResponse) error) error {
	c.listeners = append(c.listeners, listener{
		correlationID: corrID,
		callback:      cb,
	})

	return nil
}

func (c *Connection) Close() error {
	c.closeCh <- struct{}{}
	return c.conn.Close()
}
