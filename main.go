package main

import (
	"fmt"
	"time"
)

func main() {
	partition := NewPartition(0, 10000000)

	go func() {
		err := partition.Consume(0, func(message *Message) error {
			fmt.Println("o", message.Offset, "k", message.Key, "p", string(message.Payload))
			return nil
		})
		if err != nil {
			fmt.Println("error while consuming", err)
		}
	}()

	for {
		time.Sleep(time.Second * 2)

		fmt.Println("sending message")
		offset, err := partition.Push(NewMessage(
			uint64(time.Now().Unix()),
			"mykey",
			[]byte("abcadssad"),
		))

		if err != nil {
			panic(err)
		}

		println("offset", offset)
	}
}
