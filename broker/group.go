package broker

import (
	"errors"
	"godel/internal/protocol"
	"godel/options"
	"log/slog"
	"slices"
	"sync"
)

type consumerGroup struct {
	name      string
	topic     *Topic
	consumers []*consumer
	offsets   map[uint32]uint64

	mu sync.Mutex
}

func (c *consumerGroup) lock() {
	c.mu.Lock()
}

func (c *consumerGroup) unlock() {
	c.mu.Unlock()
}

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
		slog.Debug("assinging partition to consumer",
			"group", c.name,
			"partition", c.topic.partitions[i].num,
			"consumer", c.consumers[j].id,
		)
		c.consumers[j].partitions = append(c.consumers[j].partitions, c.topic.partitions[i])
		if j >= len(c.consumers) {
			j = 0
		}
	}
}

// MUST lock the consumer group before appending consumer!
func (c *consumerGroup) apendConsumer(id string, opts *options.ConsumerOptions) (*consumer, error) {
	for i := range c.consumers {
		if c.consumers[i].id == id {
			return nil, errors.New(protocol.ErrConsumerIdAlreadyExists)
		}
	}

	consumer := c.newConsumer(id, []*Partition{}, opts)
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
		return errors.New(protocol.ErrConsumerNotFound)
	}
	c.consumers[i].stop()

	// make sure its not started again,
	// but should'nt be necessary
	c.consumers[i].lock()
	defer c.consumers[i].unlock()

	c.consumers[i].close()
	c.consumers = slices.Delete(c.consumers, i, i+1)
	return nil
}

// MUST lock the consumer group before committing offset!
func (c *consumerGroup) commitOffset(partition uint32, offset uint64) error {
	c.offsets[partition] = offset
	return nil
}

func (g *consumerGroup) heartbeat(consumerID string) error {
	for i := range g.consumers {
		if g.consumers[i].id == consumerID {
			g.consumers[i].heartbeat()
			return nil
		}
	}

	return errors.New(protocol.ErrConsumerNotFound)
}

func (c *consumerGroup) delete() {
	slog.Info("stopping all consumers", "group", c.name)

	// stop all consumers before anything else
	for i := range c.consumers {
		c.consumers[i].stop()
	}

	slog.Info("removing all consumers", "group", c.name)
	for i := range c.consumers {
		// make sure its not started again,
		// but should'nt be necessary
		c.consumers[i].lock()
		defer c.consumers[i].unlock()

		c.consumers[i].close()
		c.consumers = slices.Delete(c.consumers, i, i+1)
	}
}
