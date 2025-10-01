package godel

import "time"

type CleanupPolicy int

var CleanupPolicyDelete CleanupPolicy = 0

type TopicOptions struct {
	NumPartitions   uint32        `json:"num.partitions"`
	CleanupPolicy   CleanupPolicy `json:"cleanup.policy"`
	RetentionMilli  int64         `json:"retenton.ms"`
	RetentionBytes  int32         `json:"retention.bytes"`
	SegmentBytes    int32         `json:"segment.bytes"`
	MaxMessageBytes int32         `json:"max.message.bytes"`
	// Partitioner           func(key []byte, n uint32) uint32
}

func DefaultTopicOptions() *TopicOptions {
	return &TopicOptions{
		NumPartitions:   1,                   // single partition
		CleanupPolicy:   CleanupPolicyDelete, // delete
		RetentionMilli:  604800000,           // 7 days
		RetentionBytes:  -1,                  // no limit
		SegmentBytes:    1073741824,          // 1 GiB
		MaxMessageBytes: 604800000,           // 7 days
		// Partitioner:           DefaultPartitioner,  // murmur2
	}
}

func (t *TopicOptions) WithNumPartitions(n uint32) *TopicOptions {
	t.NumPartitions = n
	return t
}

func (t *TopicOptions) WithCleanupPolicy(p CleanupPolicy) *TopicOptions {
	t.CleanupPolicy = p
	return t
}

func (t *TopicOptions) WithRetentionMilli(d time.Duration) *TopicOptions {
	t.RetentionMilli = d.Milliseconds()
	return t
}

func (t *TopicOptions) WithRetentionBytes(b int32) *TopicOptions {
	t.RetentionBytes = b
	return t
}

func (t *TopicOptions) WithSegmentBytes(b int32) *TopicOptions {
	t.SegmentBytes = b
	return t
}

func (t *TopicOptions) WithMaxMessageBytes(b int32) *TopicOptions {
	t.MaxMessageBytes = b
	return t
}

// func (t *TopicOptions) WithCustomPartitioner(p func(key []byte, n uint32) uint32) *TopicOptions {
// 	t.Partitioner = p
// 	return t
// }

type BrokerOptions struct {
	BasePath                       string `json:"base.path"`
	LogRetentionCheckIntervalMilli int64  `json:"log.retention.check.interval.ms"`
}

func DeafaultBrokerOptions() *BrokerOptions {
	return &BrokerOptions{
		BasePath:                       "./godel",
		LogRetentionCheckIntervalMilli: 300000, // 5 mins
	}
}

func (b *BrokerOptions) WithBasePath(p string) *BrokerOptions {
	b.BasePath = p
	return b
}

func (b *BrokerOptions) WithLogRetentionCheckInterval(d time.Duration) *BrokerOptions {
	b.LogRetentionCheckIntervalMilli = d.Milliseconds()
	return b
}
