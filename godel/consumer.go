package godel

import "github.com/google/uuid"

type TopicConsumer struct {
	id         string
	partitions []*Partition
	stopCh     chan struct{}
}

func NewTopicConsumer(partitions []*Partition) *TopicConsumer {
	return &TopicConsumer{
		id:         uuid.NewString(),
		partitions: partitions,
	}
}

func (c *TopicConsumer) Consume(fromBeginning bool, callback func(m *Message) error) error {
	messageCh := make(chan *Message)
	errorCh := make(chan error)

	for i := range c.partitions {
		j := i
		go func() {
			offset := uint64(0)
			if fromBeginning {
				offset = c.partitions[j].getBaseOffset()
			} else {
				offset = c.partitions[j].getNextOffset()
			}

			err := c.partitions[j].consume(offset, func(message *Message) error {
				messageCh <- message
				return nil
			})

			if err != nil {
				errorCh <- err
			}
		}()
	}

	for {
		select {
		case msg := <-messageCh:
			err := callback(msg)
			if err != nil {
				return err
			}
		case err := <-errorCh:
			return err
		case <-c.stopCh:
			return nil
		}
	}
}

func (c *TopicConsumer) Stop() {
	c.stopCh <- struct{}{}
}
