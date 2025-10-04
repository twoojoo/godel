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

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	ch := make(chan *protocol.RespCreateConsumer, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		resp, err := protocol.Deserialize[protocol.RespCreateConsumer](r.Payload)
		if err != nil {
			return err
		}

		ch <- resp
		return ErrCloseConnection
	})
	if err != nil {
		return nil, err
	}

	resp := <-ch
	return resp, nil
}
