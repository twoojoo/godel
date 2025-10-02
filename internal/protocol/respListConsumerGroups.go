package protocol

import (
	"encoding/json"
)

type RespListConsumerGroups struct {
	Groups       []ConsumerGroup `json:"groups"`
	ErrorCode    int             `json:"errorCode"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
}

type ConsumerGroup struct {
	Name      string     `json:"name"`
	Consumers []Consumer `json:"consumers"`
}

type Consumer struct {
	ID      string           `json:"name"`
	Offsets []ConsumerOffset `json:"offsets"`
}

type ConsumerOffset struct {
	Partition uint32 `json:"partition"`
	Offset    uint64 `json:"offset"`
}

func DeserializeResponseListConsumerGroups(data []byte) (*RespListConsumerGroups, error) {
	var resp RespListConsumerGroups
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespListConsumerGroups) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
