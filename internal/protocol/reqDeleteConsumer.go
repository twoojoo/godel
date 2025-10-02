package protocol

import (
	"encoding/json"
)

type ReqDeleteConsumer struct {
	Topic string `json:"topic"`
	Group string `json:"group"`
	ID    string `json:"id"`
}

func DeserializeRequestDeleteConsumer(data []byte) (*ReqDeleteConsumer, error) {
	var req ReqDeleteConsumer
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqDeleteConsumer) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
