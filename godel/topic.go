package godel

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const errTopicAlreadyExists = "topic.already.exists"

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

	partitions := make([]*Partition, 0, topicOptions.NumPartitions)
	for i := range partitions {
		partitions[i], err = newPartition(uint32(i), name, topicOptions, brokerOptions)
		if err != nil {
			// should delete created partitions here!
			return nil, err
		}
	}

	return &Topic{
		Name:          name,
		Partitions:    partitions,
		Options:       topicOptions,
		BrokerOptions: brokerOptions,
	}, nil
}

func loadTopic(name string, brokerOptions *BrokerOptions) (*Topic, error) {
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

	partitions := make([]*Partition, 0, len(partitionNums))
	for _, num := range partitionNums {
		partition, err := loadPartition(num, name, &topicOptions, brokerOptions)
		if err != nil {
			return nil, err
		}

		partitions = append(partitions, partition)
	}

	return &Topic{
		Name:          name,
		BrokerOptions: brokerOptions,
		Partitions:    partitions,
		Options:       &topicOptions,
		Consumers:     []*TopicConsumer{},
	}, nil
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
