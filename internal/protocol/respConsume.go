package protocol

import (
	"encoding/json"
)

type RespConsume struct {
	Messages []RespConsumeMessage `json:"messages"`
}

type RespConsumeMessage struct {
	Key          string  `json:"key"`
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	Payload      []byte  `json:"payload"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

func DeserializeResponseConsume(data []byte) (*RespConsume, error) {
	var resp RespConsume
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespConsume) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
