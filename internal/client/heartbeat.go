package client

import (
	"fmt"
	"godel/internal/protocol"
)

func (c *Connection) Heartbeat(topic, group string, consumerID string) (*protocol.RespHeartbeat, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqHeartbeat{
		Topic:      topic,
		Group:      group,
		ConsumerID: consumerID,
	}

	reqBuf, err := req.Serialize()
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdHeartbeat,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
	}

	ch := make(chan *protocol.RespHeartbeat, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		// if msg.CorrelationID != r.CorrelationID {
		// 	return nil
		// }

		resp, err := protocol.DeserializeResponseHeartbeat(r.Payload)
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
