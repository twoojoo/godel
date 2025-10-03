package main

import (
	"context"
	"encoding/json"
	"fmt"
	"godel/internal/client"
	"os"

	"github.com/urfave/cli/v3"
)

var cmdListTopics = &cli.Command{
	Name:    "list",
	Aliases: []string{"ls"},
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "name-filter",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		nameFilter := cmd.StringArg("name-filter")

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err != client.ErrCloseConnection {
				fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			}
		})
		if err != nil {
			return err
		}

		resp, err := conn.ListTopics(nameFilter)
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
