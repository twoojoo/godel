package godel

type Broker struct {
	Optrions *BrokerOptions
	Topics   []*Topic
}

func NewBroker(opts ...*BrokerOptions) *Broker {
	if len(opts) == 0 {
		opts = append(opts, DeafaultBrokerOptions())
	}

	return &Broker{
		Optrions: opts[0],
	}
}

func (b *Broker) CreateTopic(name string, opts *TopicOptions) (*Topic, error) {
	topic, err := NewTopic(name, opts)
	if err != nil {
		return nil, err
	}

	b.Topics = append(b.Topics, topic)

	return topic, nil
}
