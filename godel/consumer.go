package godel

import "github.com/google/uuid"

type TopicConsumer struct {
	StopCh     chan struct{}
	ID         string
	Partitions []*Partition
}

func NewTopicConsumer(partitions []*Partition) *TopicConsumer {
	return &TopicConsumer{
		ID:         uuid.NewString(),
		Partitions: partitions,
	}
}

func (c *TopicConsumer) Consume(fromBeginning bool, callback func(m *Message) error) error {
	messageCh := make(chan *Message)
	errorCh := make(chan error)

	for i := range c.Partitions {
		j := i
		go func() {
			offset := uint64(0)
			if fromBeginning {
				offset = c.Partitions[j].GetBaseOffset()
			} else {
				offset = c.Partitions[j].GetNextOffset()
			}

			err := c.Partitions[j].Consume(offset, func(message *Message) error {
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
		case <-c.StopCh:
			return nil
		}
	}
}

func (c *TopicConsumer) Stop() {
	c.StopCh <- struct{}{}
}
