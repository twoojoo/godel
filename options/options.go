package options

import (
	"os"
	"time"

	yaml "github.com/goccy/go-yaml"
)

type CleanupPolicy string

var CleanupPolicyDelete CleanupPolicy = "delete"

type TopicOptions struct {
	NumPartitions   uint32        `json:"num.partitions"`
	CleanupPolicy   CleanupPolicy `json:"cleanup.policy"`
	RetentionMilli  int64         `json:"retenton.ms"`
	RetentionBytes  int32         `json:"retention.bytes"`
	SegmentBytes    int32         `json:"segment.bytes"`
	MaxMessageBytes int32         `json:"max.message.bytes"`
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

func MergeTopicOptions(o1, o2 *TopicOptions) {
	if o1.NumPartitions == 0 {
		o1.NumPartitions = o2.NumPartitions
	}

	if o1.CleanupPolicy == "" {
		o1.CleanupPolicy = o2.CleanupPolicy
	}

	if o1.RetentionMilli == 0 {
		o1.RetentionMilli = o2.RetentionMilli
	}

	if o1.RetentionBytes == 0 {
		o1.RetentionBytes = o2.RetentionBytes
	}

	if o1.SegmentBytes == 0 {
		o1.SegmentBytes = o2.SegmentBytes
	}

	if o1.MaxMessageBytes == 0 {
		o1.MaxMessageBytes = o2.MaxMessageBytes
	}
}

type BrokerOptions struct {
	BasePath                       string `json:"base.path"`
	LogRetentionCheckIntervalMilli int64  `json:"log.retention.check.interval.ms"`
}

func DeafaultBrokerOptions() *BrokerOptions {
	return &BrokerOptions{
		BasePath:                       "./godel_data",
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

func LoadBrokerOptionsFromYaml(path string) (*BrokerOptions, error) {
	blob, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var opts BrokerOptions
	err = yaml.Unmarshal(blob, &opts)
	if err != nil {
		return nil, err
	}

	MergeBrokerOptions(&opts, DeafaultBrokerOptions())

	return &opts, nil
}

func MergeBrokerOptions(o1, o2 *BrokerOptions) {
	if o1.BasePath == "" {
		o1.BasePath = o2.BasePath
	}

	if o1.LogRetentionCheckIntervalMilli == 0 {
		o1.LogRetentionCheckIntervalMilli = o2.LogRetentionCheckIntervalMilli
	}
}

type ConsumerOptions struct {
	SessionTimeoutMilli    int64 `json:"session.timeout.ms"`
	HeartbeatIntervalMilli int64 `json:"heartbeat.interval.milli"`
}

func DefaulcConsumerOption() *ConsumerOptions {
	return &ConsumerOptions{
		SessionTimeoutMilli:    10000, // 10 secs
		HeartbeatIntervalMilli: 3000,  // 3 secs
	}
}

func (o *ConsumerOptions) WithSessionTimeout(d time.Duration) *ConsumerOptions {
	o.SessionTimeoutMilli = d.Milliseconds()
	return o
}

func (o *ConsumerOptions) WithHeartbeatInterval(d time.Duration) *ConsumerOptions {
	o.HeartbeatIntervalMilli = d.Milliseconds()
	return o
}

func MergeConsumerOptions(o1, o2 *ConsumerOptions) {
	if o1.HeartbeatIntervalMilli == 0 {
		o1.HeartbeatIntervalMilli = o2.HeartbeatIntervalMilli
	}

	if o1.SessionTimeoutMilli == 0 {
		o1.SessionTimeoutMilli = o2.SessionTimeoutMilli
	}
}
