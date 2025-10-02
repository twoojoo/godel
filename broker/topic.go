package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"godel/options"
	"log/slog"
	"os"

	"github.com/google/uuid"
)

const errTopicAlreadyExists = "topic.already.exists"
const errPartitionsNumMismatch = "num.partition.mismatch"
const errConsumerGroupNotFound = "consumer.group.not.found"
const errConsumerNotFound = "consumer.not.found"

type Topic struct {
	name           string
	partitions     []*Partition
	options        *options.TopicOptions
	brokerOptions  *options.BrokerOptions
	consumerGroups map[string]*consumerGroup
	// consumers     []*TopicConsumer
}

func (t *Topic) persistOptions() error {
	topicOptsPath := fmt.Sprintf("%s/%s/options.json", t.brokerOptions.BasePath, t.name)

	if t.options.NumPartitions < 1 {
		t.options.NumPartitions = 1
	}

	optionsBytes, err := json.Marshal(t.options)
	if err != nil {
		return err
	}

	err = os.WriteFile(topicOptsPath, optionsBytes, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (t *Topic) loadOptions() error {
	topicOptsPath := fmt.Sprintf("%s/%s/options.json", t.brokerOptions.BasePath, t.name)

	optsBytes, err := os.ReadFile(topicOptsPath)
	if err != nil {
		return err
	}

	var topicOptions options.TopicOptions
	err = json.Unmarshal(optsBytes, &topicOptions)
	if err != nil {
		return err
	}

	t.options = &topicOptions
	return nil
}

func newTopic(name string, topicOptions *options.TopicOptions, brokerOptions *options.BrokerOptions) (*Topic, error) {
	topicPath := fmt.Sprintf("%s/%s", brokerOptions.BasePath, name)
	// topicOptsPath := fmt.Sprintf("%s/%s/options.json", brokerOptions.BasePath, name)

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

	topic := &Topic{
		name:          name,
		options:       topicOptions,
		brokerOptions: brokerOptions,
	}

	err := topic.persistOptions()
	if err != nil {
		return nil, err
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

func loadTopic(name string, brokerOptions *options.BrokerOptions, newTopicOptions *options.TopicOptions) (*Topic, error) {
	topicPath := fmt.Sprintf("%s/%s", brokerOptions.BasePath, name)

	if _, err := os.Stat(topicPath); os.IsNotExist(err) {
		err = os.Mkdir(topicPath, 0755)

		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	topic := &Topic{
		name:           name,
		brokerOptions:  brokerOptions,
		options:        newTopicOptions,
		consumerGroups: map[string]*consumerGroup{},
	}

	if newTopicOptions != nil {
		err := topic.persistOptions()
		if err != nil {
			return nil, err
		}
	} else {
		err := topic.loadOptions()
		if err != nil {
			return nil, err
		}
	}

	partitionsNames, err := listSubfolders(topicPath)
	if err != nil {
		return nil, err
	}

	partitionNums, err := strSliceToUint32(partitionsNames)
	if err != nil {
		return nil, err
	}

	if len(partitionNums) == 0 {
		err = topic.initializePartitions()
		if err != nil {
			return nil, err
		}

		return topic, nil
	}

	if len(partitionNums) != int(topic.options.NumPartitions) {
		return nil, errors.New(errPartitionsNumMismatch)
	}

	partitions := make([]*Partition, 0, len(partitionNums))
	for _, num := range partitionNums {
		partition, err := newPartition(num, name, topic.options, brokerOptions)
		if err != nil && err.Error() == errPartitionAlreadyExists {
			partition, err = loadPartition(num, name, topic.options, brokerOptions)
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

func (t *Topic) Options() *options.TopicOptions {
	return t.options
}

func (t *Topic) produce(message *Message) (uint64, uint32, error) {
	partitionNumber := DefaultPartitioner([]byte(message.key), t.options.NumPartitions)

	var partition *Partition
	for i := range t.partitions {
		if t.partitions[i].num == partitionNumber {
			partition = t.partitions[i]
		}
	}

	offset, err := partition.push(message)
	if err != nil {
		return 0, 0, nil
	}

	return offset, partitionNumber, nil
}

// func (t *Topic) Consume(offset uint64) error {
// 	// temp consume from first partition only
// 	return t.partitions[0].consume(offset, callback)
// }

func (t *Topic) createConsumer(group, id string, fromBeginning bool) (*consumer, error) {
	if id == "" { // generate new id when group is not specified
		id = uuid.NewString()
	}

	if group == "" {
		group = id
	}

	isGroupNew := false
	if cg, ok := t.consumerGroups[group]; ok {
		cg.stop()
	} else {
		cg := consumerGroup{
			name:      group,
			topic:     t,
			consumers: []*consumer{},
		}

		t.consumerGroups[group] = &cg
		isGroupNew = true
	}

	t.consumerGroups[group].lock()
	defer t.consumerGroups[group].unlock()

	id = group + "-" + id
	consumer, err := t.consumerGroups[group].apendConsumer(id, fromBeginning)
	if err != nil {
		if len(t.consumerGroups[group].consumers) == 0 && isGroupNew {
			delete(t.consumerGroups, group)
		}
		return nil, err
	}

	slog.Info("consumer crated, consumer group rebalancing",
		"group", group,
		"consumers", len(t.consumerGroups[group].consumers),
		"consumer", consumer.id,
	)

	t.consumerGroups[group].rebalance()

	slog.Info("rebalancing done", "group", group)
	return consumer, nil
}

func (t *Topic) removeConsumer(group string, id string) error {
	if _, ok := t.consumerGroups[group]; !ok {
		return errors.New(errConsumerGroupNotFound)
	}

	t.consumerGroups[group].stop()

	t.consumerGroups[group].lock()
	defer t.consumerGroups[group].unlock()

	err := t.consumerGroups[group].removeConsumer(id)
	if err != nil {
		return err
	}

	if len(t.consumerGroups[group].consumers) == 0 {
		slog.Warn("consumer group has zero consumers", "group", group)
		return nil
	}

	slog.Info("consumer removed, consumer group rebalancing",
		"group", group,
		"consumers", len(t.consumerGroups[group].consumers),
		"consumer", id,
	)

	t.consumerGroups[group].rebalance()

	slog.Info("rebalancing done", "group", group)
	return nil
}

func (t *Topic) listConsumerGroups() map[string]*consumerGroup {
	return t.consumerGroups
}

func (t *Topic) commitOffset(group string, partition uint32, offset uint64) error {
	if _, ok := t.consumerGroups[group]; !ok {
		return errors.New(errConsumerGroupNotFound)
	}

	t.consumerGroups[group].lock()
	defer t.consumerGroups[group].unlock()

	err := t.consumerGroups[group].commitOffset(partition, offset)
	if err != nil {
		return err
	}

	return nil
}
