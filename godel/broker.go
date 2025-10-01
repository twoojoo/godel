package godel

import (
	"errors"
	"os"
)

const errTopicNotFound = "topic.not.found"

type Broker struct {
	Options *BrokerOptions
	Topics  []*Topic
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
			Options: opts[0],
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
		broker.Topics, err = broker.loadTopics()
		if err != nil {
			errorCh <- err
		}

		readyCh <- struct{}{}
	}()

	select {
	case err := <-errorCh:
		return nil, err
	case <-readyCh:
		return &broker, nil
	}
}

func (b *Broker) GetOrCreateTopic(name string, opts ...*TopicOptions) (*Topic, error) {
	if len(opts) == 0 {
		opts = append(opts, DefaultTopicOptions())
	}

	topic, err := newTopic(name, opts[0], b.Options)
	if err != nil && err.Error() == errTopicAlreadyExists {
		for i := range b.Topics {
			if b.Topics[i].Name == name {
				return b.Topics[i], nil
			}
		}

		// here we should warn when options are not overridable
	}
	if err != nil {
		return nil, err
	}

	b.Topics = append(b.Topics, topic)

	return topic, nil
}

func (b *Broker) Produce(topic string, message *Message) (uint64, error) {
	for i := range b.Topics {
		if b.Topics[i].Name == topic {
			return b.Topics[i].produce(message)
		}
	}

	return 0, errors.New(errTopicNotFound)
}

// loadTopics scans broker path for topics folders and
// recursively loads eache topic from its own path.
func (b *Broker) loadTopics() ([]*Topic, error) {
	topicNames, err := listSubfolders(b.Options.BasePath)
	if err != nil {
		return nil, err
	}

	topics := make([]*Topic, 0, len(topicNames))
	for i := range topicNames {
		topic, err := loadTopic(topicNames[i], b.Options)
		if err != nil {
			return nil, err
		}

		topics = append(topics, topic)
	}

	return topics, nil
}
