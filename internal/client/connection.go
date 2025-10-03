package client

import (
	"bufio"
	"errors"
	"godel/internal/protocol"
	"net"
	"sync"
)

var errCloseConnection = errors.New("close.connection")

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

	mu sync.Mutex
}

func ConnectToBroker(addr string, onError func(*Connection, error)) (*Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	requests := make(chan *protocol.BaseRequest, 100)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	c := &Connection{
		reader:    reader,
		writer:    writer,
		requests:  requests,
		listeners: []listener{},
		conn:      conn,
	}

	c.startListening()
	c.startSending()

	return c, nil
}

func (c *Connection) startListening() {
	go func() {
		for {
			msg, err := protocol.DeserializeResponse(c.reader)
			if err != nil {
				c.onError(c, err)
				return
			}

			// fmt.Println("received response", msg.CorrelationID)

			// c.mu.Lock()
			// defer c.mu.Unlock()

			for i := range c.listeners {
				if c.listeners[i].correlationID == msg.CorrelationID {
					err = c.listeners[i].callback(msg)
					if err != nil {
						c.onError(c, err)
						continue
					}

					if err == errCloseConnection {
						c.Close()
						return
					}
				}
			}
		}
	}()
}

func (c *Connection) startSending() {
	go func() {
		for {
			select {
			case <-c.closeCh:
				return
			case m := <-c.requests:
				// fmt.Println("sending request", m.Cmd, m.CorrelationID)
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

func (c *Connection) ReadMessage(corrID int32, cb func(r *protocol.BaseResponse) error) error {
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
