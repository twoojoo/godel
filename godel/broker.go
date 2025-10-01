package godel

import (
	"errors"
	"os"
)

const errTopicNotFound = "topic.not.found"

type Broker struct {
	options *BrokerOptions
	topics  []*Topic
}

func NewBroker(opts ...*BrokerOptions) (*Broker, error) {
	readyCh := make(chan struct{})
	errorCh := make(chan error)

	var broker Broker

	go func() {
		if len(opts) == 0 {
			opts = append(opts, DeafaultBrokerOptions())
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

func (b *Broker) GetOrCreateTopic(name string, opts ...*TopicOptions) (*Topic, error) {
	if len(opts) == 0 {
		opts = append(opts, DefaultTopicOptions())
	}

	topic, err := newTopic(name, opts[0], b.options)
	if err != nil && err.Error() == errTopicAlreadyExists {
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

func (b *Broker) Produce(topic string, message *Message) (uint64, error) {
	for i := range b.topics {
		if b.topics[i].name == topic {
			return b.topics[i].produce(message)
		}
	}

	return 0, errors.New(errTopicNotFound)
}
