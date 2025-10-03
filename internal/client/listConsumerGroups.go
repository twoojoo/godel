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

	reqBuf, err := req.Serialize()
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdListGroups,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	ch := make(chan *protocol.RespListConsumerGroups, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		// if msg.CorrelationID != r.CorrelationID {
		// 	return nil
		// }

		resp, err := protocol.DeserializeResponseListConsumerGroups(r.Payload)
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
