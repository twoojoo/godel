package godel

import (
	"fmt"
	"os"
)

const errTopicAlreadyExists = "topic.already.exists"

type Topic struct {
	Name       string
	Partitions []*Partition
	Options    *TopicOptions
}

func NewTopic(name string, opts ...*TopicOptions) (*Topic, error) {
	topicPath := fmt.Sprintf("./%s", name)

	if _, err := os.Stat(topicPath); os.IsNotExist(err) {
		err = os.Mkdir(topicPath, 0755)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		return nil, fmt.Errorf(errTopicAlreadyExists)
	}

	if len(opts) == 0 {
		opts = append(opts, DefaultTopicOptions())
	}

	if opts[0].NumPartitions < 1 {
		opts[0].NumPartitions = 1
	}

	var err error
	partitions := make([]*Partition, opts[0].NumPartitions)
	for i := range partitions {
		partitions[i], err = NewPartition(i, name, opts[0])
		if err != nil {
			// should delete created partitions here!
			return nil, err
		}
	}

	return &Topic{
		Name:       name,
		Partitions: partitions,
		Options:    opts[0],
	}, nil
}

func (t *Topic) Produce(message *Message) (uint64, error) {
	partitionNumber := t.Options.Partitioner([]byte(message.Key), t.Options.NumPartitions)

	var partition *Partition
	for i := range t.Partitions {
		if t.Partitions[i].ID == int(partitionNumber) {
			partition = t.Partitions[i]
		}
	}

	return partition.Push(message)
}

func (t *Topic) Consume(offset uint64, callback func(message *Message) error) error {
	// temp consume from first partition only
	return t.Partitions[0].Consume(offset, callback)
}
