package main

import (
	"fmt"
	"godel/godel"
	"strconv"
	"time"
)

func main() {
	options := godel.DeafaultBrokerOptions().
		WithBasePath("./test")

	broker, err := godel.NewBroker(options)
	if err != nil {
		panic(err)
	}

	_, err = broker.GetOrCreateTopic("mytopic")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		fmt.Println("producing message", i)
		offset, err := broker.Produce("mytopic", godel.NewMessage(
			uint64(time.Now().Unix()),
			[]byte("key"+strconv.Itoa(i)),
			[]byte("abracadabra"),
		))
		if err != nil {
			panic(err)
		}

		fmt.Println("offset", offset)
	}
}
