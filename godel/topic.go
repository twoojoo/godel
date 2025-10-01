package godel

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

const errTopicAlreadyExists = "topic.already.exists"
const errPartitionsNumMismatch = "num.partition.mismatch"

type Topic struct {
	name          string
	partitions    []*Partition
	options       *TopicOptions
	brokerOptions *BrokerOptions
	consumers     []*TopicConsumer
}

func newTopic(name string, topicOptions *TopicOptions, brokerOptions *BrokerOptions) (*Topic, error) {
	topicPath := fmt.Sprintf("%s/%s", brokerOptions.BasePath, name)
	topicOptsPath := fmt.Sprintf("%s/%s/options.json", brokerOptions.BasePath, name)

	if _, err := os.Stat(topicPath); os.IsNotExist(err) {
		err = os.Mkdir(topicPath, 0755)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		return nil, errors.New(errTopicAlreadyExists)
	}

	slog.Info("initializing topic", "topic", name)

	optionsBytes, err := json.Marshal(topicOptions)
	if err != nil {
		return nil, err
	}

	err = os.WriteFile(topicOptsPath, optionsBytes, 0644)
	if err != nil {
		return nil, err
	}

	if topicOptions.NumPartitions < 1 {
		topicOptions.NumPartitions = 1
	}

	topic := &Topic{
		name:          name,
		options:       topicOptions,
		brokerOptions: brokerOptions,
	}

	err = topic.initializePartitions()
	if err != nil {
		return nil, err
	}

	return topic, nil
}

func (t *Topic) initializePartitions() error {
	var err error
	partitions := make([]*Partition, t.options.NumPartitions)
	for i := range partitions {
		partitions[i], err = newPartition(uint32(i), t.name, t.options, t.brokerOptions)
		if err != nil {
			// should delete created partitions here!
			return err
		}
	}

	t.partitions = partitions
	return nil
}

func loadTopic(name string, brokerOptions *BrokerOptions) (*Topic, error) {
	slog.Info("loading topic", "topic", name)

	topicPath := fmt.Sprintf("%s/%s", brokerOptions.BasePath, name)
	topicOptsPath := fmt.Sprintf("%s/%s/options.json", brokerOptions.BasePath, name)

	if _, err := os.Stat(topicPath); os.IsNotExist(err) {
		err = os.Mkdir(topicPath, 0755)

		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	optsBytes, err := os.ReadFile(topicOptsPath)
	if err != nil {
		return nil, err
	}

	var topicOptions TopicOptions
	err = json.Unmarshal(optsBytes, &topicOptions)
	if err != nil {
		return nil, err
	}

	partitionsNames, err := listSubfolders(topicPath)
	if err != nil {
		return nil, err
	}

	partitionNums, err := strSliceToUint32(partitionsNames)
	if err != nil {
		return nil, err
	}

	topic := &Topic{
		name:          name,
		brokerOptions: brokerOptions,
		options:       &topicOptions,
		consumers:     []*TopicConsumer{},
	}

	if len(partitionNums) == 0 {
		slog.Info("zero partitions found", "topic", name, "partitions", topic.options.NumPartitions)

		err = topic.initializePartitions()
		if err != nil {
			return nil, err
		}

		return topic, nil
	}

	if len(partitionNums) != int(topicOptions.NumPartitions) {
		return nil, errors.New(errPartitionsNumMismatch)
	}

	partitions := make([]*Partition, 0, len(partitionNums))
	for _, num := range partitionNums {
		partition, err := newPartition(num, name, &topicOptions, brokerOptions)
		if err != nil && err.Error() == errPartitionAlreadyExists {
			partition, err = loadPartition(num, name, &topicOptions, brokerOptions)
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}

		partitions = append(partitions, partition)
	}

	topic.partitions = partitions

	return topic, nil
}

func (t *Topic) Options() *TopicOptions {
	return t.options
}

func (t *Topic) produce(message *Message) (uint64, error) {
	partitionNumber := DefaultPartitioner([]byte(message.key), t.options.NumPartitions)

	var partition *Partition
	for i := range t.partitions {
		if t.partitions[i].num == partitionNumber {
			partition = t.partitions[i]
		}
	}

	return partition.push(message)
}

func (t *Topic) consume(offset uint64, callback func(message *Message) error) error {
	// temp consume from first partition only
	return t.partitions[0].Consume(offset, callback)
}

func (t *Topic) registerConsumer(consumer *TopicConsumer) {
	t.consumers = append(t.consumers, consumer)
}
