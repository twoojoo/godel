package broker

import (
	"errors"
	"slices"
	"sync"
)

const errConsumerIdAlreadyExists = "consumer.id.already.exists"

type consumerGroup struct {
	name      string
	topic     *Topic
	consumers []*consumer

	offsets map[uint32]uint64

	mu sync.Mutex
}

func (c *consumerGroup) lock() {
	c.mu.Lock()
}

func (c *consumerGroup) unlock() {
	c.mu.Unlock()
}

// // starts all consumer group consumers
// func (c *consumerGroup) start(cb func(m *Message) error) {
// 	for i := range c.consumers {
// 		c.consumers[i].start(cb)
// 	}
// }

// stops all consumer groups consumers
func (c *consumerGroup) stop() {
	for i := range c.consumers {
		c.consumers[i].stop()
	}
}

// reassign partitions
//
// MUST stop the consumer group befare calling this!
//
// MUST call the .lock() and .unlock() methods!
func (c *consumerGroup) rebalance() {
	shuffleSlice(c.consumers)

	// clear assigned partitions
	for i := range c.consumers {
		c.consumers[i].lock()
		defer c.consumers[i].unlock()

		c.consumers[i].partitions = []*Partition{}
	}

	// round robin to reassign partitions
	j := 0
	for i := range c.topic.partitions {
		c.consumers[j].partitions = append(c.consumers[j].partitions, c.topic.partitions[i])
		if j >= len(c.consumers) {
			j = 0
		}
	}
}

// MUST lock the consumer group before appending consumer!
func (c *consumerGroup) apendConsumer(id string, fromBeginning bool) (*consumer, error) {
	for i := range c.consumers {
		if c.consumers[i].id == id {
			return nil, errors.New(errConsumerIdAlreadyExists)
		}
	}

	consumer := newConsumer(id, []*Partition{}, fromBeginning)
	c.consumers = append(c.consumers, consumer)
	return consumer, nil
}

// MUST lock the consumer group before appending consumer!
func (c *consumerGroup) removeConsumer(id string) error {
	i := -1
	for j := range c.consumers {
		if c.consumers[j].id == id {
			i = j
		}
	}

	if i == -1 {
		return errors.New(errConsumerNotFound)
	}

	c.consumers[i].stop()

	// make sure its not started again,
	// but should'nt be necessary
	c.consumers[i].lock()
	defer c.consumers[i].unlock()

	c.consumers = slices.Delete(c.consumers, i, i+1)
	return nil
}
