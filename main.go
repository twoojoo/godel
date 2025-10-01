package main

import (
	"godel/godel"
	"log/slog"
	"time"
)

func main() {
	options := godel.DeafaultBrokerOptions().
		WithBasePath("./test")

	broker, err := godel.NewBroker(options)
	if err != nil {
		panic(err)
	}

	topic, err := broker.GetOrCreateTopic("mytopic")
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(2 * time.Second)

		err = topic.Consume(5, func(m *godel.Message) error {
			slog.Info("message", "offset", m.Offset(), "key", m.Key(), "payload", m.Payload())
			time.Sleep(time.Millisecond * 500)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}()

	// for i := 0; i < 10; i++ {
	// 	offset, err := broker.Produce("mytopic", godel.NewMessage(
	// 		uint64(time.Now().Unix()),
	// 		[]byte("key"+strconv.Itoa(i)),
	// 		[]byte("abracadabra"),
	// 	))
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	fmt.Println("offset", offset)
	// }

	select {}
}
