package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"

	"github.com/urfave/cli/v3"
)

var cmdDeleteTopic = &cli.Command{
	Name:    "remove",
	Aliases: []string{"rm", "delete", "del"},
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		topic := cmd.StringArg("topic")
		if topic == "" {
			return errors.New("topic is required")
		}

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err != client.ErrCloseConnection {
				fmt.Println("error", err)
			}
		})
		if err != nil {
			return err
		}

		resp, err := conn.DeleteTopic(topic)
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
