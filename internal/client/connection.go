package client

import (
	"bufio"
	"godel/internal/protocol"
	"net"
)

type Connection struct {
	reader *bufio.Reader
	writer *bufio.Writer
	conn   net.Conn
}

func ConnectToBroker(addr string) (*Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Connection{
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		conn:   conn,
	}, nil
}

func (c *Connection) SendMessage(m *protocol.BaseRequest) error {
	ser := m.Serialize()

	_, err := c.writer.Write(ser)
	if err != nil {
		return err
	}

	return c.writer.Flush()
}

func (c *Connection) ReadMessage(cb func(r *protocol.BaseResponse) error) error {
	for {
		msg, err := protocol.DeserializeResponse(c.reader)
		if err != nil {
			return err
		}

		cb(msg)
	}
}
