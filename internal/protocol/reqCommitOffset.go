package protocol

import (
	"encoding/json"
)

type ReqCommitOffset struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"parition"`
	Offset    uint64 `json:"offset"`
	Group     string `json:"consumerGroup"`
}

func DeserializeCommitOffsetRequest(data []byte) (*ReqCommitOffset, error) {
	var req ReqCommitOffset
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (r *ReqCommitOffset) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
