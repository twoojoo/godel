package client

import (
	"godel/internal/protocol"
)

func (c *Connection) GetTopic(topic string) (*protocol.RespGetTopic, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqGetTopic{
		Topic: topic,
	}

	reqBuf, err := protocol.Serialize(req)
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdGetTopic,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	ch := make(chan *protocol.RespGetTopic, 1)
	err = c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		resp, err := protocol.Deserialize[protocol.RespGetTopic](r.Payload)
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
