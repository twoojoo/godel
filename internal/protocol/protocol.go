package protocol

import (
	"encoding/binary"
	"io"
)

const (
	CmdProduce int16 = 0
	CmdFetch   int16 = 1

	OffsetCommit int16 = 8
	CreateTopics int16 = 19
)

type BaseRequest struct {
	Cmd           int16
	ApiVersion    int16
	CorrelationID int32
	Payload       []byte
}

func DeserializeRequest(r io.Reader) (*BaseRequest, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(r, lenBuf)
	if err != nil {
		return nil, err
	}

	reqLen := binary.BigEndian.Uint32(lenBuf)

	req := make([]byte, reqLen)
	_, err = io.ReadFull(r, req)
	if err != nil {
		return nil, err
	}

	cmd := int16(binary.BigEndian.Uint16(req))
	apiV := int16(binary.BigEndian.Uint16(req[2:]))
	corrID := int32(binary.BigEndian.Uint32(req[4:]))
	payload := req[8:]

	return &BaseRequest{
		Cmd:           cmd,
		ApiVersion:    apiV,
		CorrelationID: corrID,
		Payload:       payload,
	}, nil
}

func ProcessRequest(r *BaseRequest) ([]byte, error) {
	return []byte{}, nil
}

type BaseResponse struct {
	CorrelationID int32
	Payload       []byte
}

func (r *BaseResponse) Serialize() []byte {
	totalLen := 4 + 4 + len(r.Payload)
	buf := make([]byte, 0, totalLen)

	buf = binary.BigEndian.AppendUint32(buf, uint32(totalLen))
	buf = binary.BigEndian.AppendUint32(buf, uint32(r.CorrelationID))
	buf = append(buf, r.Payload...)

	return buf
}
