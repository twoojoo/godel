package protocol

import (
	"encoding/json"
	"godel/options"
)

type ReqCreateTopic struct {
	Topics    []ReqCreateTopicTopic `json:"topics"`
	TimeoutMs uint64                `json:"timeoutMs"`
}

type ReqCreateTopicTopic struct {
	Name    string               `json:"name"`
	Configs options.TopicOptions `json:"config"`
}

func DeserializeRequestCreateTopic(data []byte) (*ReqCreateTopic, error) {
	var req ReqCreateTopic
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqCreateTopic) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
