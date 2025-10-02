package broker

import (
	"sync"
)

// Any modification to the consumer object must
// e done after locking it with the .lock() method.
// When the operation is done you can call .unlock().
type consumer struct {
	id            string
	partitions    []*Partition
	fromBeginning bool
	started       bool

	stoppedCh chan struct{}
	stopCh    chan struct{}

	mu sync.Mutex
}

func newConsumer(id string, partitions []*Partition, fromBeginning bool) *consumer {
	return &consumer{
		id:            id,
		partitions:    partitions,
		fromBeginning: fromBeginning,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
	}
}

func (c *consumer) lock() {
	c.mu.Lock()
}

func (c *consumer) unlock() {
	c.mu.Unlock()
}

func (c *consumer) start(callback func(m *Message) error) error {
	c.mu.Lock()
	defer func() {
		c.started = false
	}()
	defer c.mu.Unlock()

	c.started = true
	messageCh := make(chan *Message)
	errorCh := make(chan error)

	for i := range c.partitions {
		j := i
		go func() {
			offset := uint64(0)
			if c.fromBeginning {
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
			c.stoppedCh <- struct{}{}
			return nil
		}
	}
}

// send a stop signal and wait for actual stopped signal
func (c *consumer) stop() {
	if !c.started {
		return
	}

	c.stopCh <- struct{}{}
	<-c.stoppedCh
}
