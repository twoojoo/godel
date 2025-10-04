package protocol

import "godel/options"

type ReqCommitOffset struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"parition"`
	Offset    uint64 `json:"offset"`
	Group     string `json:"consumerGroup"`
}

type ReqConsume struct {
	ID              string `json:"id"`
	Topic           string `json:"topic"`
	Group           string `json:"group"`
	FromBeginning   bool   `json:"fromBeginning"`
	TimeoutMs       uint64 `json:"timeoutMs"`
	ConsumerOptions options.ConsumerOptions
}

type ReqCreateTopics struct {
	Topics    []ReqCreateTopicTopic `json:"topics"`
	TimeoutMs uint64                `json:"timeoutMs"`
}

type ReqCreateTopicTopic struct {
	Name    string               `json:"name"`
	Configs options.TopicOptions `json:"config"`
}

type ReqDeleteConsumer struct {
	Topic string `json:"topic"`
	Group string `json:"group"`
	ID    string `json:"id"`
}

type ReqHeartbeat struct {
	Topic      string `json:"topic"`
	Group      string `json:"consumerGroup"`
	ConsumerID string `json:"consumerId"`
}

type ReqListConsumerGroups struct {
	Topic string `json:"topic"`
}

type ReqListTopics struct {
	NameFilter string `json:"nameFilter"`
}

type ReqProduce struct {
	Topic     string              `json:"topic"`
	Messages  []ReqProduceMessage `json:"message"`
	TimeoutMs uint64              `json:"timeoutMs"`
}

type ReqProduceMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type RespConsume struct {
	Messages     []RespConsumeMessage `json:"messages"`
	ErrorCode    int                  `json:"errorCode"`
	ErrorMessage string               `json:"errorMessage,omitempty"`
}

type RespConsumeMessage struct {
	Key          string  `json:"key"`
	Group        string  `json:"conumerGroup"`
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	Payload      []byte  `json:"payload"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}
