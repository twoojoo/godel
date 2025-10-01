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
	Name          string
	Partitions    []*Partition
	Options       *TopicOptions
	BrokerOptions *BrokerOptions
	Consumers     []*TopicConsumer
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
		Name:          name,
		Options:       topicOptions,
		BrokerOptions: brokerOptions,
	}

	err = topic.initializePartitions()
	if err != nil {
		return nil, err
	}

	return topic, nil
}

func (t *Topic) initializePartitions() error {
	var err error
	partitions := make([]*Partition, t.Options.NumPartitions)
	for i := range partitions {
		partitions[i], err = newPartition(uint32(i), t.Name, t.Options, t.BrokerOptions)
		if err != nil {
			// should delete created partitions here!
			return err
		}
	}

	t.Partitions = partitions
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
		Name:          name,
		BrokerOptions: brokerOptions,
		Options:       &topicOptions,
		Consumers:     []*TopicConsumer{},
	}

	if len(partitionNums) == 0 {
		slog.Info("zero partitions found", "topic", name, "partitions", topic.Options.NumPartitions)

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

	topic.Partitions = partitions

	return topic, nil
}

func (t *Topic) produce(message *Message) (uint64, error) {
	partitionNumber := DefaultPartitioner([]byte(message.Key), t.Options.NumPartitions)

	var partition *Partition
	for i := range t.Partitions {
		if t.Partitions[i].ID == partitionNumber {
			partition = t.Partitions[i]
		}
	}

	return partition.push(message)
}

func (t *Topic) consume(offset uint64, callback func(message *Message) error) error {
	// temp consume from first partition only
	return t.Partitions[0].Consume(offset, callback)
}

func (t *Topic) registerConsumer(consumer *TopicConsumer) {
	t.Consumers = append(t.Consumers, consumer)
}
