package protocol

import (
	"encoding/json"
	"godel/options"
)

type RespListTopics struct {
	Topics       []ListTopicsTopic `json:"topics"`
	ErrorCode    int               `json:"errorCode"`
	ErrorMessage string            `json:"errorMessage,omitempty"`
}

type ListTopicsTopic struct {
	Name       string               `json:"name"`
	Partitions []uint32             `json:"partitions"`
	Groups     []string             `json:"consumerGroups"`
	Options    options.TopicOptions `json:"config"`
}

func DeserializeListTopicsResponse(data []byte) (*RespListTopics, error) {
	var resp RespListTopics
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespListTopics) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
