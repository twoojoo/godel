package client

import (
	"godel/internal/protocol"
)

func (c *Connection) DeleteTopic(topic string) (*protocol.RespDeleteTopic, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqDeleteConsumer{
		Topic: topic,
	}

	reqBuf, err := protocol.Serialize(req)
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdDeleteTopic,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	respCh := make(chan *protocol.RespDeleteTopic)
	errCh := make(chan error)

	close := c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) {
		resp, err := protocol.Deserialize[protocol.RespDeleteTopic](r.Payload)
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
