package protocol

import (
	"encoding/json"
)

type RespHeartbeat struct {
	ConsumerID   string `json:"consumerId"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func DeserializeResponseHeartbeat(data []byte) (*RespHeartbeat, error) {
	var resp RespHeartbeat
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespHeartbeat) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
