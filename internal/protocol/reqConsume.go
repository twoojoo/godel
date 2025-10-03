package protocol

import (
	"encoding/json"
	"godel/options"
)

type ReqConsume struct {
	ID              string `json:"id"`
	Topic           string `json:"topic"`
	Group           string `json:"group"`
	FromBeginning   bool   `json:"fromBeginning"`
	TimeoutMs       uint64 `json:"timeoutMs"`
	ConsumerOptions options.ConsumerOptions
}

func DeserializeRequestConsume(data []byte) (*ReqConsume, error) {
	var req ReqConsume
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqConsume) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
