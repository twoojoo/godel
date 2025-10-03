package client

import (
	"fmt"
	"godel/internal/protocol"
)

func (c *Connection) Produce(topic string, key, payload []byte) (*protocol.RespCreateTopics, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqProduce{
		Topic: topic,
		Messages: []protocol.ReqProduceMessage{
			{
				Key:   key,
				Value: []byte(payload),
			},
		},
	}

	reqBuf, err := req.Serialize()
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdProduce,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
	}

	ch := make(chan *protocol.RespCreateTopics, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		// if msg.CorrelationID != r.CorrelationID {
		// 	return nil
		// }

		resp, err := protocol.DeserializeResponseCreateTopic(r.Payload)
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
