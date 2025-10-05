package protocol

import "godel/options"

type RespHeartbeat struct {
	ConsumerID   string `json:"consumerId"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type RespCreateTopics struct {
	Topics []RespCreateTopicTopic `json:"topics,omitempty"`
}

type RespCreateTopicTopic struct {
	Name         string `json:"name"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type RespDeleteConsumer struct {
	ID           string `json:"id"`
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type RespCommitOffset struct {
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

type RespListConsumerGroups struct {
	Groups       []ConsumerGroup `json:"groups"`
	ErrorCode    int             `json:"errorCode"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
}

type ConsumerGroup struct {
	Name      string                `json:"name"`
	Consumers []Consumer            `json:"consumers,omitempty"`
	Offsets   []ConsumerGroupOffset `json:"offsets,omitempty"`
}

type Consumer struct {
	ID         string   `json:"id"`
	Partitions []uint32 `json:"partitions,omitempty"`
}

type ConsumerGroupOffset struct {
	Partition uint32 `json:"partition"`
	Offset    uint64 `json:"offset"`
}

type RespListTopics struct {
	Topics       []Topic `json:"topics,omitempty"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

type Topic struct {
	Name       string               `json:"name"`
	Partitions []uint32             `json:"partitions,omitempty"`
	Groups     []string             `json:"consumerGroups,omitempty"`
	Options    options.TopicOptions `json:"config"`
}

type RespProduce struct {
	Messages []RespProduceMessage `json:"messages,omitempty"`
}

type RespProduceMessage struct {
	Key          string  `json:"key"`
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

type RespConsume struct {
	Stop         bool                 `json:"stop"`
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

type RespCreateConsumer struct {
	ID           string `json:"id"`
	Topic        string `json:"topic"`
	Group        string `json:"conumerGroup"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type RespDeleteTopic struct {
	Topic        string `json:"topic"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type RespNotifyRebalance struct {
	Group string `json:"group"`
}

type RespGetTopic struct {
	Topic        Topic  `json:"topic"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}
