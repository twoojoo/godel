package protocol

import (
	"encoding/json"
)

type RespDeleteConsumer struct {
	ID           string `json:"id"`
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func DeserializeResponseDeleteConsumer(data []byte) (*RespDeleteConsumer, error) {
	var resp RespDeleteConsumer
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespDeleteConsumer) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
