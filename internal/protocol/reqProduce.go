package protocol

import (
	"encoding/json"
)

type ReqProduce struct {
	Topic     string              `json:"topic"`
	Messages  []ReqProduceMessage `json:"message"`
	TimeoutMs uint64              `json:"timeoutMs"`
}

type ReqProduceMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func DeserializeRequestProduce(data []byte) (*ReqProduce, error) {
	var req ReqProduce
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqProduce) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
