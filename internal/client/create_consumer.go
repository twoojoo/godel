package client

import (
	"godel/internal/protocol"
	"godel/options"
)

func (c *Connection) CreateConsumer(topic, group string, opts *options.ConsumerOptions, id ...string) (*protocol.RespCreateConsumer, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	options.MergeConsumerOptions(opts, options.DefaulcConsumerOption())

	consumerID := ""
	if len(id) != 0 {
		consumerID = id[0]
	}

	req := protocol.ReqCreateConsumer{
		ID:      consumerID,
		Topic:   topic,
		Group:   group,
		Options: *opts,
	}

	reqBuf, err := protocol.Serialize(req)
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdCreateConsumer,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	c.SendMessage(msg)

	respCh := make(chan *protocol.RespCreateConsumer)
	errCh := make(chan error)

	close := c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) {
		resp, err := protocol.Deserialize[protocol.RespCreateConsumer](r.Payload)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}, true)

	defer close()

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	select {
	case err := <-errCh:
		return nil, err
	case resp := <-respCh:
		return resp, nil
	}
}
