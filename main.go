package main

import (
	"fmt"
	"godel/broker"
	"log/slog"
	"strconv"
	"time"
)

func main() {
	options := broker.DeafaultBrokerOptions().
		WithBasePath("./test").
		WithLogRetentionCheckInterval(time.Minute * 5)

	gb, err := broker.NewBroker(options)
	if err != nil {
		panic(err)
	}

	topicOptions := broker.DefaultTopicOptions().
		WithRetentionMilli(time.Minute * 5)

	topic, err := gb.GetOrCreateTopic("mytopic", topicOptions)
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(2 * time.Second)

		err = topic.Consume(5, func(m *broker.Message) error {
			slog.Info("message", "offset", m.Offset(), "key", m.Key(), "payload", m.Payload())
			time.Sleep(time.Millisecond * 500)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}()

	for i := 0; i < 10; i++ {
		offset, err := gb.Produce("mytopic", broker.NewMessage(
			uint64(time.Now().Unix()),
			[]byte("key"+strconv.Itoa(i)),
			[]byte("abracadabra"),
		))
		if err != nil {
			panic(err)
		}

		fmt.Println("offset", offset)
	}

	select {}
}
