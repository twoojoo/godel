package client

import (
	"godel/internal/protocol"
	"godel/options"
)

func (c *Connection) CreateTopics(name string, opts *options.TopicOptions) (*protocol.RespCreateTopics, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	options.MergeTopicOptions(opts, options.DefaultTopicOptions())

	req := protocol.ReqCreateTopics{
		Topics: []protocol.ReqCreateTopicTopic{
			{
				Name:    name,
				Configs: *opts,
			},
		},
	}

	reqBuf, err := protocol.Serialize(req)
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdCreateTopics,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	ch := make(chan *protocol.RespCreateTopics, 1)
	err = c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		resp, err := protocol.Deserialize[protocol.RespCreateTopics](r.Payload)
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
