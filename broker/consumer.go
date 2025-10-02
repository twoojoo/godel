package broker

import (
	"sync"

	"github.com/google/uuid"
)

// Any modification to the consumer object must
// e done after locking it with the .lock() method.
// When the operation is done you can call .unlock().
type consumer struct {
	id            string
	partitions    []*Partition
	offsets       map[uint32]uint64
	fromBeginning bool

	stoppedCh chan struct{}
	stopCh    chan struct{}

	mu sync.Mutex
}

func newConsumer(partitions []*Partition, fromBeginning bool) *consumer {
	return &consumer{
		id:            uuid.NewString(),
		partitions:    partitions,
		fromBeginning: fromBeginning,
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
	defer c.mu.Unlock()

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
			c.offsets[msg.partition] = msg.offset + 1
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
	c.stopCh <- struct{}{}
	<-c.stoppedCh
}
