package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"

	"github.com/urfave/cli/v3"
)

var cmdDeleteConsumer = &cli.Command{
	Name: "delete",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
		&cli.StringArg{
			Name: "id",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		topic := cmd.StringArg("topic")
		if topic == "" {
			return errors.New("topic must be provided")
		}

		id := cmd.StringArg("id")
		if topic == "" {
			return errors.New("id must be provided")
		}

		group, err := getGroupFromConsumerID(id)
		if err != nil {
			return err
		}

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err != client.ErrCloseConnection {
				fmt.Println("error", err)
			}
		})
		if err != nil {
			return err
		}

		resp, err := conn.DeleteConsumer(topic, group, id)
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
