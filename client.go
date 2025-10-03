package godel

import (
	"godel/internal/client"
	"godel/internal/protocol"
	"godel/options"
	"strconv"
)

type GodelClient struct {
	addr string
	conn *client.Connection
}

func Connect(host string, port int) (*GodelClient, error) {
	addr := host + ":" + strconv.Itoa(port)

	conn, err := client.ConnectToBroker(addr, func(c *client.Connection, err error) {
		c.Close()

		// to handle
	})

	if err != nil {
		return nil, err
	}

	return &GodelClient{
		addr: addr,
		conn: conn,
	}, nil
}

func (c *GodelClient) CreateTopic(name string, opts *options.TopicOptions) (*protocol.RespCreateTopics, error) {
	return c.conn.CreateTopics(name, opts)
}
