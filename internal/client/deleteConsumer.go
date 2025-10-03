package client

import (
	"godel/internal/protocol"
)

func (c *Connection) DeleteConsumer(topic, group, id string) (*protocol.RespDeleteConsumer, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqDeleteConsumer{
		ID:    id,
		Group: group,
		Topic: topic,
	}

	reqBuf, err := req.Serialize()
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdLeaveGroup,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	ch := make(chan *protocol.RespDeleteConsumer, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {

		resp, err := protocol.DeserializeResponseDeleteConsumer(r.Payload)
		if err != nil {
			return err
		}

		ch <- resp
		return errCloseConnection
	})
	if err != nil {
		return nil, err
	}

	resp := <-ch
	return resp, nil
}
