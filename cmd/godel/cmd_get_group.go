package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"
	"os"

	"github.com/urfave/cli/v3"
)

var cmdGetConsumerGroup = &cli.Command{
	Name: "get",
	// Aliases: []string{"ls"},
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
		&cli.StringArg{
			Name: "name",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) error {
		topic := cmd.StringArg("topic")
		if topic == "" {
			return errors.New("topic name is required")
		}

		name := cmd.StringArg("name")
		if name == "" {
			return errors.New("group name is required")
		}

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			// if err != client.ErrCloseConnection {
			fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			// }
		})
		if err != nil {
			return err
		}

		resp, err := conn.GetConsumerGroup(topic, name)
		if err != nil {
			return err
		}

		bytes, err := json.Marshal(&resp)
		if err != nil {
			return err
		}

		fmt.Println(string(bytes))
		return nil
	},
}
