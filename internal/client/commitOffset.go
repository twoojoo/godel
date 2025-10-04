package client

import (
	"fmt"
	"godel/internal/protocol"
)

func (c *Connection) CommitOffset(topic, group string, partition uint32, offset uint64) (*protocol.RespCommitOffset, error) {
	corrID, err := GenerateCorrelationID()
	if err != nil {
		return nil, err
	}

	req := protocol.ReqCommitOffset{
		Topic:     topic,
		Group:     group,
		Partition: partition,
		Offset:    offset,
	}

	reqBuf, err := protocol.Serialize(req)
	if err != nil {
		return nil, err
	}

	msg := &protocol.BaseRequest{
		Cmd:           protocol.CmdOffsetCommit,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	err = c.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
	}

	ch := make(chan *protocol.RespCommitOffset, 1)
	err = c.ReadMessage(msg.CorrelationID, func(r *protocol.BaseResponse) error {
		// if msg.CorrelationID != r.CorrelationID {
		// 	return nil
		// }

		resp, err := protocol.Deserialize[protocol.RespCommitOffset](r.Payload)
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
