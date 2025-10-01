package main

type Topic struct {
	Name       string
	Partitions []*Partition
	Options    *TopicOptions
}

func NewTopic(name string, opts ...*TopicOptions) *Topic {
	if len(opts) == 0 {
		opts = append(opts, DefaultTopicOptions())
	}

	if opts[0].NumPartitions < 1 {
		opts[0].NumPartitions = 1
	}

	partitions := make([]*Partition, opts[0].NumPartitions)
	for i := range partitions {
		partitions[i] = NewPartition(i, opts[0])
	}

	return &Topic{
		Name:       name,
		Partitions: partitions,
		Options:    opts[0],
	}
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
