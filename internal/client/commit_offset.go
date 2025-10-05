package client

import (
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
		Cmd:           protocol.CmdCommitOffset,
		ApiVersion:    0,
		CorrelationID: corrID,
		Payload:       reqBuf,
	}

	respCh := make(chan *protocol.RespCommitOffset)
	errCh := make(chan error)

	close := c.AppendListener(msg.CorrelationID, func(r *protocol.BaseResponse) {
		resp, err := protocol.Deserialize[protocol.RespCommitOffset](r.Payload)
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
