package protocol

import (
	"encoding/json"
)

type RespCreateTopics struct {
	Topics []RespCreateTopicTopic `json:"topics"`
}

type RespCreateTopicTopic struct {
	Name         string `json:"name"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func DeserializeResponseCreateTopic(data []byte) (*RespCreateTopics, error) {
	var resp RespCreateTopics
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespCreateTopics) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
