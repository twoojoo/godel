package main

import (
	"fmt"
	"time"
)

func main() {
	topic := NewTopic("mytopic")

	go func() {
		err := topic.Consume(0, func(message *Message) error {
			fmt.Println(
				"o", message.Offset,
				"k", string(message.Key),
				"p", string(message.Payload),
			)
			return nil
		})
		if err != nil {
			fmt.Println("error while consuming", err)
		}
	}()

	for {
		time.Sleep(time.Second * 1)

		fmt.Println("sending message")
		offset, err := topic.Produce(NewMessage(
			uint64(time.Now().Unix()),
			[]byte("mykey"),
			[]byte("abcadssad"),
		))

		if err != nil {
			panic(err)
		}

		println("offset", offset)
	}
}
