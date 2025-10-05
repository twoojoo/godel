package client

import (
	"godel/internal/protocol"
)

func (c *Connection) ListConsumerGroups(topic string) (*protocol.RespListConsumerGroups, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqListConsumerGroups{
		Topic: topic,
	}

	reqBuf, err := protocol.Serialize(req)
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdListConsumerGroups,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	respCh := make(chan *protocol.RespListConsumerGroups)
	errCh := make(chan error)

	close := c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) {
		resp, err := protocol.Deserialize[protocol.RespListConsumerGroups](r.Payload)
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
