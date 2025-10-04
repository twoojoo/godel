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

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	ch := make(chan *protocol.RespDeleteTopic, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		resp, err := protocol.Deserialize[protocol.RespDeleteTopic](r.Payload)
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
