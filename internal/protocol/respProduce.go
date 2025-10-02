package protocol

import (
	"encoding/json"
)

type RespProduce struct {
	Messages []RespProduceMessage `json:"messages"`
}

type RespProduceMessage struct {
	Key          string  `json:"key"`
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

func DeserializeResponseProduce(data []byte) (*RespProduce, error) {
	var resp RespProduce
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespProduce) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
