package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/protocol"
	"godel/options"
	"log/slog"
	"os"
	"sync"

	"github.com/google/uuid"
)

type Topic struct {
	name           string
	partitions     []*Partition
	options        *options.TopicOptions
	brokerOptions  *options.BrokerOptions
	consumerGroups map[string]*consumerGroup

	mu sync.Mutex
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
		return nil, errors.New(protocol.ErrTopicAlreadyExists)
	}

	slog.Info("initializing topic", "topic", name)

	topic := &Topic{
		name:           name,
		options:        topicOptions,
		brokerOptions:  brokerOptions,
		consumerGroups: map[string]*consumerGroup{},
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

	topic.mu.Lock()
	defer topic.mu.Unlock()

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
		return nil, errors.New(protocol.ErrPartitionsNumMismatch)
	}

	partitions := make([]*Partition, 0, len(partitionNums))
	for _, num := range partitionNums {
		partition, err := newPartition(num, name, topic.options, brokerOptions)
		if err != nil && err.Error() == protocol.ErrPartitionAlreadyExists {
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

	topicState, err := topic.loadState()
	if err != nil {
		return nil, err
	}

	// load existing consumer groups with their offsets
	if len(topicState.ConsumerGroups) > 0 {
		groupNames := make([]string, len(topicState.ConsumerGroups))
		groupOffsets := make([]map[uint32]uint64, len(topicState.ConsumerGroups))

		for i := range topicState.ConsumerGroups {
			stateGroupPartitions := getMapKeys(topicState.ConsumerGroups[i].Offsets)
			consistentGroup := slicesEqualUnordered(partitionNums, stateGroupPartitions)
			if !consistentGroup && len(topicState.ConsumerGroups[i].Offsets) != 0 {
				return nil, errors.New(protocol.ErrConsumerGroupsPartitionsMismatch)
			}

			groupNames = append(groupNames, topicState.ConsumerGroups[i].Name)
			groupOffsets = append(groupOffsets, topicState.ConsumerGroups[i].Offsets)
		}

		_, err = topic.createConsumerGroups(groupNames, groupOffsets)
		if err != nil {
			return nil, err
		}
	}

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

func (t *Topic) createConsumerGroups(names []string, offsets []map[uint32]uint64) ([]*consumerGroup, error) {
	if t.mu.TryLock() {
		defer t.mu.Unlock()
	}

	cgs := make([]*consumerGroup, len(names))

	if offsets != nil && len(offsets) != len(names) {
		return nil, errors.New(protocol.ErrConsumerGroupsOffsetsMismatch)
	}

	for i := range names {
		cg := consumerGroup{
			name:      names[i],
			topic:     t,
			consumers: []*consumer{},
			offsets:   map[uint32]uint64{},
		}

		if len(offsets) != 0 {
			cg.offsets = offsets[i]
		}

		t.consumerGroups[names[i]] = &cg

		err := t.persistState()
		if err != nil {
			return nil, err
		}

		cgs[i] = &cg
	}

	return cgs, nil
}

func (t *Topic) getConsumer(group, id string) (*consumer, error) {
	if group == "" {
		return nil, errors.New(protocol.ErrMissingGroupName)
	}

	if id == "" {
		return nil, errors.New(protocol.ErrMissingConsumerId)
	}

	cg, ok := t.consumerGroups[group]
	if !ok {
		return nil, errors.New(protocol.ErrConsumerGroupNotFound)
	}

	for i := range cg.consumers {
		if cg.consumers[i].id == id {
			return cg.consumers[i], nil
		}
	}

	return nil, errors.New(protocol.ErrConsumerNotFound)
}

func (t *Topic) createConsumer(group, id string, opts *options.ConsumerOptions) (*consumer, error) {
	if group == "" {
		return nil, errors.New(protocol.ErrMissingGroupName)
	}

	if id == "" { // generate new id when group is not specified
		id = group + uuid.NewString()
	}

	isGroupNew := false
	if cg, ok := t.consumerGroups[group]; ok {
		cg.stop()
	} else {
		isGroupNew = true
		_, err := t.createConsumerGroups([]string{group}, nil)
		if err != nil {
			return nil, err
		}
	}

	t.consumerGroups[group].lock()
	defer t.consumerGroups[group].unlock()

	consumer, err := t.consumerGroups[group].apendConsumer(id, opts)
	if err != nil {
		if len(t.consumerGroups[group].consumers) == 0 && isGroupNew {
			delete(t.consumerGroups, group)
		}
		return nil, err
	}

	consumer.startHearbeatChecks(func(id string) {
		slog.Info("consumer expired, removing", "group", group, "consumer", id)
		err := t.removeConsumer(group, id)
		if err != nil {
			slog.Error("failed to remove consumer after heartbeat check", "group", group, "consumer", id, "error", err)
		}
	})

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
	if group == "" {
		return errors.New(protocol.ErrMissingGroupName)
	}

	if id == "" {
		return errors.New(protocol.ErrMissingConsumerId)
	}

	if _, ok := t.consumerGroups[group]; !ok {
		return errors.New(protocol.ErrConsumerGroupNotFound)
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
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.consumerGroups
}

func (t *Topic) commitOffset(group string, partition uint32, offset uint64) error {
	if group == "" {
		return errors.New(protocol.ErrMissingGroupName)
	}

	if _, ok := t.consumerGroups[group]; !ok {
		return errors.New(protocol.ErrConsumerGroupNotFound)
	}

	t.consumerGroups[group].lock()
	defer t.consumerGroups[group].unlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.consumerGroups[group].commitOffset(partition, offset)
	if err != nil {
		return err
	}

	err = t.persistState()
	if err != nil {
		return err
	}

	return nil
}

func (t *Topic) heartbeat(group, id string) error {
	if group == "" {
		return errors.New(protocol.ErrMissingGroupName)
	}

	if id == "" {
		return errors.New(protocol.ErrMissingConsumerId)
	}

	if _, ok := t.consumerGroups[group]; !ok {
		return errors.New(protocol.ErrConsumerGroupNotFound)
	}

	err := t.consumerGroups[group].heartbeat(id)
	if err != nil {
		return err
	}

	return nil
}

func (t *Topic) loadState() (*topicState, error) {
	topicStatePath := fmt.Sprintf("%s/%s/state.json", t.brokerOptions.BasePath, t.name)
	stateBytes, err := os.ReadFile(topicStatePath)
	if err != nil {
		return nil, err
	}

	var topicState topicState
	err = json.Unmarshal(stateBytes, &topicState)
	if err != nil {
		return nil, err
	}

	return &topicState, nil
}

func (t *Topic) persistState() error {
	statePath := fmt.Sprintf("%s/%s/state.json", t.brokerOptions.BasePath, t.name)

	state := topicState{
		ConsumerGroups: []topicStateGroup{},
	}

	for i := range t.consumerGroups {
		state.ConsumerGroups = append(state.ConsumerGroups, topicStateGroup{
			Name:    t.consumerGroups[i].name,
			Offsets: t.consumerGroups[i].offsets,
		})
	}

	stateBytes, err := json.Marshal(&state)
	if err != nil {
		return err
	}

	err = os.WriteFile(statePath, stateBytes, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (t *Topic) delete() error {
	slog.Info("deleting topic", "topic", t.name)

	t.mu.Lock()
	defer t.mu.Unlock()

	for name := range t.consumerGroups {
		t.consumerGroups[name].lock()
		t.consumerGroups[name].stop()
		t.consumerGroups[name].delete()
		t.consumerGroups[name].unlock()
		delete(t.consumerGroups, name)
	}

	slog.Info("all consumer groups detached", "topic", t.name)

	// remove all files
	// options, state and all partitions files
	topicPath := fmt.Sprintf("%s/%s", t.brokerOptions.BasePath, t.name)
	err := os.RemoveAll(topicPath)
	if err != nil {
		slog.Error("error while deleting topic files", "topic", t.name)
	}

	return nil
}
