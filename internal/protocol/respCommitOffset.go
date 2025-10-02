package protocol

import (
	"encoding/json"
)

type RespCommitOffset struct {
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

func DeserializeResponseCommitOffset(data []byte) (*RespCommitOffset, error) {
	var resp RespCommitOffset
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (r *RespCommitOffset) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
