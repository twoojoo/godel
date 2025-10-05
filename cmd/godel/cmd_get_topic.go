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

var cmdGetTopic = &cli.Command{
	Name: "get",
	// Aliases: []string{"ls"},
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) error {
		topic := cmd.StringArg("topic")
		if topic == "" {
			return errors.New("topic name is required")
		}

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			// if err != client.ErrCloseConnection {
			fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			// }
		})
		if err != nil {
			return err
		}

		resp, err := conn.GetTopic(topic)
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
