package broker

import (
	"fmt"
	"godel/options"
	"sync"
	"time"
)

// Any modification to the consumer object must
// e done after locking it with the .lock() method.
// When the operation is done you can call .unlock().
type consumer struct {
	id            string
	group         *consumerGroup
	partitions    []*Partition
	fromBeginning bool
	started       bool
	lastHeartbeat time.Time
	options       *options.ConsumerOptions

	stoppedCh chan struct{}
	stopCh    chan struct{}
	deleteCh  chan struct{}

	mu         sync.Mutex
	hearbeatMu sync.Mutex
}

func (c *consumerGroup) newConsumer(id string, partitions []*Partition, fromBeginning bool, opts *options.ConsumerOptions) *consumer {
	consumer := &consumer{
		id:            id,
		group:         c,
		partitions:    partitions,
		fromBeginning: fromBeginning,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		deleteCh:      make(chan struct{}, 1),
		options:       opts,
		lastHeartbeat: time.Now(),
	}

	c.consumers = append(c.consumers, consumer)
	return consumer
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
			offset := c.group.offsets[c.partitions[i].num] + 1 // consume next message

			if offset == 0 && c.fromBeginning {
				offset = c.partitions[j].getBaseOffset()
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

func (c *consumer) close() {
	c.deleteCh <- struct{}{}
}

func (c *consumer) heartbeat() {
	c.hearbeatMu.Lock()
	defer c.hearbeatMu.Unlock()

	c.lastHeartbeat = time.Now()
}

func (c *consumer) heartbeatCheck() bool {
	c.hearbeatMu.Lock()
	defer c.hearbeatMu.Unlock()

	age := time.Since(c.lastHeartbeat)
	maxAge := time.Duration(c.options.SessionTimeoutMilli) * time.Millisecond
	fmt.Println(age, maxAge)
	return age > maxAge
}

func (c *consumer) startHearbeatChecks(onExpiration func(id string)) {
	go func() {
		for {
			time.Sleep(time.Duration(c.options.SessionTimeoutMilli) * time.Millisecond)

			select {
			case <-c.deleteCh:
				return
			default:
				expired := c.heartbeatCheck()
				if expired {
					onExpiration(c.id)
				}
			}
		}
	}()
}
