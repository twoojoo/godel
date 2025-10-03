package protocol

import (
	"encoding/json"
)

type ReqHeartbeat struct {
	Topic      string `json:"topic"`
	Group      string `json:"consumerGroup"`
	ConsumerID string `json:"consumerId"`
}

func DeserializeHeartbeatRequest(data []byte) (*ReqHeartbeat, error) {
	var req ReqHeartbeat
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqHeartbeat) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
