package broker

import (
	"errors"
	"godel/internal/protocol"
	"godel/options"
	"log/slog"
	"os"
	"sync"
)

type Broker struct {
	options *options.BrokerOptions
	topics  []*Topic

	mu sync.RWMutex
}

func NewBroker(opts ...*options.BrokerOptions) (*Broker, error) {
	readyCh := make(chan struct{})
	errorCh := make(chan error)

	var broker Broker

	go func() {
		if len(opts) == 0 {
			opts = append(opts, options.DeafaultBrokerOptions())
		}

		broker = Broker{
			options: opts[0],
		}

		// create broker path if it doesn't exists yet
		if _, err := os.Stat(opts[0].BasePath); os.IsNotExist(err) {
			err = os.Mkdir(opts[0].BasePath, 0755)
			if err != nil {
				errorCh <- err
			}
		} else if err != nil {
			errorCh <- err
		}

		// load all topics from fs
		var err error
		broker.topics, err = broker.loadTopics()
		if err != nil {
			errorCh <- err
		}

		readyCh <- struct{}{}
	}()

	select {
	case err := <-errorCh:
		return nil, err
	case <-readyCh:
		broker.scheduleRetentionCheck()
		return &broker, nil
	}
}

// loadTopics scans broker path for topics folders and
// recursively loads eache topic from its own path.
func (b *Broker) loadTopics() ([]*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topicNames, err := listSubfolders(b.options.BasePath)
	if err != nil {
		return nil, err
	}

	topics := make([]*Topic, 0, len(topicNames))
	for i := range topicNames {
		topic, err := loadTopic(topicNames[i], b.options, nil)
		if err != nil {
			return nil, err
		}

		topics = append(topics, topic)
	}

	return topics, nil
}

func (b *Broker) CreateTopic(name string, opts ...*options.TopicOptions) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(opts) == 0 {
		opts = append(opts, options.DefaultTopicOptions())
	}

	topic, err := newTopic(name, opts[0], b.options)
	if err != nil {
		return nil, err
	}

	b.topics = append(b.topics, topic)
	return topic, nil
}

func (b *Broker) GetTopic(name string) (*Topic, error) {
	b.mu.RLock()

	for i := range b.topics {
		if b.topics[i].name == name {
			return b.topics[i], nil
		}
	}

	return nil, errors.New(protocol.ErrTopicNotFound)
}

func (b *Broker) GetOrCreateTopic(name string, opts ...*options.TopicOptions) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(opts) == 0 {
		opts = append(opts, options.DefaultTopicOptions())
	}

	topic, err := newTopic(name, opts[0], b.options)
	if err != nil && err.Error() == protocol.ErrTopicAlreadyExists {
		for i := range b.topics {
			if b.topics[i].name == name {
				return loadTopic(name, b.options, opts[0])
			}
		}

		// here we should warn when options are not overridable
	}
	if err != nil {
		return nil, err
	}

	b.topics = append(b.topics, topic)

	return topic, nil
}

func (b *Broker) Produce(topic string, message *Message) (uint64, uint32, error) {
	b.mu.RLock()
	for i := range b.topics {
		if b.topics[i].name == topic {
			return b.topics[i].produce(message)
		}
	}

	return 0, 0, errors.New(protocol.ErrTopicNotFound)
}

func (b *Broker) Run(port int) error {
	err := b.runServer(port)
	if err != nil {
		slog.Error("error while starting server", "error", err)
	}
	return err
}

func (b *Broker) RUnlock() {
	b.mu.RUnlock()
}

func (b *Broker) listTopics() []*Topic {
	b.mu.RLock()
	return b.topics
}

func (b *Broker) deleteTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	i := -1
	for j := range b.topics {
		if b.topics[j].name == topic {
			b.topics[j].delete()
			i = j
			break
		}
	}

	if i == -1 {
		return errors.New(protocol.ErrTopicNotFound)
	}

	b.topics = append(b.topics[:i], b.topics[i+1:]...)
	slog.Info("topic fully deleted", "topic", topic)
	return nil
}
