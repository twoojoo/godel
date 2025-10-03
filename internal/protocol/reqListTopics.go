package protocol

import (
	"encoding/json"
)

type ReqListTopics struct {
	NameFilter string `json:"nameFilter"`
}

func DeserializeReqListTopics(data []byte) (*ReqListTopics, error) {
	var req ReqListTopics
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqListTopics) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
