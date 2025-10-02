package client

import (
	"bufio"
	"godel/internal/protocol"
	"net"

	"github.com/urfave/cli/v3"
)

type connection struct {
	reader *bufio.Reader
	writer *bufio.Writer
	conn   net.Conn
}

func getAddr(cmd *cli.Command) string {
	host := "localhost"
	port := "9090"

	if hostOpt := cmd.String("host"); hostOpt != "" {
		host = hostOpt
	}

	if portOpt := cmd.String("port"); portOpt != "" {
		port = portOpt
	}

	return host + ":" + port
}

func connectToBroker(cmd *cli.Command) (*connection, error) {
	conn, err := net.Dial("tcp", getAddr(cmd))
	if err != nil {
		return nil, err
	}

	return &connection{
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		conn:   conn,
	}, nil
}

func (c *connection) sendMessage(m *protocol.BaseRequest) error {
	ser := m.Serialize()

	_, err := c.writer.Write(ser)
	if err != nil {
		return err
	}

	return c.writer.Flush()
}

func (c *connection) readMessage(cb func(r *protocol.BaseResponse) error) error {
	for {
		msg, err := protocol.DeserializeResponse(c.reader)
		if err != nil {
			return err
		}

		cb(msg)
	}
}
