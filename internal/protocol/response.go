package protocol

import (
	"encoding/binary"
	"io"
)

type BaseResponse struct {
	CorrelationID int32
	Payload       []byte
}

func DeserializeResponse(r io.Reader) (*BaseResponse, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(r, lenBuf)
	if err != nil {
		return nil, err
	}

	respLen := binary.BigEndian.Uint32(lenBuf)

	req := make([]byte, respLen)
	_, err = io.ReadFull(r, req)
	if err != nil {
		return nil, err
	}

	corrID := int32(binary.BigEndian.Uint32(req))
	payload := req[4:]

	return &BaseResponse{
		CorrelationID: corrID,
		Payload:       payload,
	}, nil
}

func (r *BaseResponse) Serialize() []byte {
	totalLen := 4 + len(r.Payload)
	buf := make([]byte, 0, 4+totalLen)

	buf = binary.BigEndian.AppendUint32(buf, uint32(totalLen))
	buf = binary.BigEndian.AppendUint32(buf, uint32(r.CorrelationID))
	buf = append(buf, r.Payload...)

	return buf
}
