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

	respCh := make(chan *protocol.RespCreateTopics)
	errCh := make(chan error)

	close := c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) {
		resp, err := protocol.Deserialize[protocol.RespCreateTopics](r.Payload)
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
