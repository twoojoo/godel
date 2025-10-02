package protocol

import (
	"encoding/json"
)

type ReqConsume struct {
	Topic         string `json:"topic"`
	FromBeginning bool   `json:"fromBeginning"`
	TimeoutMs     uint64 `json:"timeoutMs"`
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
