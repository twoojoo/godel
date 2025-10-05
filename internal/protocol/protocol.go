package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

const (
	CmdProduce         int16 = 1
	CmdConsume         int16 = 2
	CmdListTopics      int16 = 3
	CmdCreateConsumer  int16 = 4
	CmdDeleteTopic     int16 = 5
	CmdNotifyRebalabce int16 = 6
	CmdGetTopic        int16 = 7
	CmdCommitOffset    int16 = 8
	CmdHeartbeat       int16 = 9
	CmdDeleteConsumer  int16 = 10
	CmdListGroups      int16 = 11
	CmdCreateTopics    int16 = 12
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

func (r *BaseRequest) Serialize() []byte {
	totalLen := 2 + 2 + 4 + len(r.Payload)
	buf := make([]byte, 0, totalLen)

	buf = binary.BigEndian.AppendUint32(buf, uint32(totalLen))
	buf = binary.BigEndian.AppendUint16(buf, uint16(r.Cmd))
	buf = binary.BigEndian.AppendUint16(buf, uint16(r.ApiVersion))
	buf = binary.BigEndian.AppendUint32(buf, uint32(r.CorrelationID))
	buf = append(buf, r.Payload...)

	return buf
}

type BaseResponse struct {
	Cmd           int16
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

	cmd := int16(binary.BigEndian.Uint16(req))
	corrID := int32(binary.BigEndian.Uint32(req[2:]))
	payload := req[6:]

	return &BaseResponse{
		Cmd:           cmd,
		CorrelationID: corrID,
		Payload:       payload,
	}, nil
}

func (r *BaseResponse) Serialize() []byte {
	totalLen := 2 + 4 + len(r.Payload)
	buf := make([]byte, 0, 4+totalLen)

	buf = binary.BigEndian.AppendUint32(buf, uint32(totalLen))
	buf = binary.BigEndian.AppendUint16(buf, uint16(r.Cmd))
	buf = binary.BigEndian.AppendUint32(buf, uint32(r.CorrelationID))
	buf = append(buf, r.Payload...)

	return buf
}

func Serialize(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, errors.New("nil message")
	}

	return json.Marshal(data)
}

func Deserialize[T any](b []byte) (*T, error) {
	var out T
	err := json.Unmarshal(b, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}
