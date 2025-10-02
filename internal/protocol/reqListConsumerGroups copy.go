package protocol

import (
	"encoding/json"
)

type ReqListConsumerGroups struct {
	Topic string `json:"topic"`
}

func DeserializeReqListConsumerGroups(data []byte) (*ReqListConsumerGroups, error) {
	var req ReqListConsumerGroups
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqListConsumerGroups) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
