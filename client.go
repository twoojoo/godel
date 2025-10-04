package godel

import (
	"errors"
	"godel/internal/client"
	"godel/internal/protocol"
	"godel/options"
	"strconv"
)

type GodelClient struct {
	addr string
	conn *client.Connection
}

func Client(host string, port int) (*GodelClient, error) {
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

func (c *GodelClient) ListTopics(nameFilter ...string) ([]*Topic, error) {
	filter := ""
	if len(nameFilter) > 0 {
		filter = nameFilter[0]
	}

	resp, err := c.conn.ListTopics(filter)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode == 0 {
		topics := make([]*Topic, len(resp.Topics))
		for i := range resp.Topics {
			topics[i] = &Topic{
				name: resp.Topics[i].Name,
			}
		}
		return topics, nil
	}
	return nil, errors.New(resp.ErrorMessage)
}

func (c *GodelClient) GetTopic(name string, opts *options.TopicOptions) (*Topic, error) {
	resp, err := c.conn.GetTopic(name)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode == 0 {
		return &Topic{name: resp.Topic.Name}, nil
	}
	return nil, errors.New(resp.ErrorMessage)
}

func (c *GodelClient) GetOrCreateTopic(name string, opts *options.TopicOptions) (*Topic, error) {
	resp1, err := c.conn.GetTopic(name)
	if err != nil {
		return nil, err
	}
	if resp1.ErrorCode == 0 {
		return &Topic{name: resp1.Topic.Name}, nil
	}
	if resp1.ErrorMessage != protocol.ErrTopicNotFound {
		return nil, errors.New(resp1.ErrorMessage)
	}

	resp2, err := c.conn.CreateTopics(name, opts)
	if err != nil {
		return nil, err
	}
	if len(resp2.Topics) != 1 {
		return nil, errors.New("unexpected topics number in response")
	}
	if resp2.Topics[0].ErrorCode != 0 {
		return nil, errors.New(resp2.Topics[0].ErrorMessage)
	}

	return &Topic{name: resp2.Topics[0].Name}, nil
}

func (c *GodelClient) DeleteTopic(name string) error {
	resp, err := c.conn.DeleteTopic(name)
	if err != nil {
		return err
	}
	if resp.ErrorCode == 0 {
		return nil
	}
	return errors.New(resp.ErrorMessage)
}
