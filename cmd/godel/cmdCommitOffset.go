package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"

	"github.com/urfave/cli/v3"
)

var cmdCommitoffset = &cli.Command{
	Name: "commit",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
		&cli.StringArg{
			Name: "group",
		},
		&cli.Uint32Arg{
			Name: "partition",
		},
		&cli.Uint64Arg{
			Name: "offset",
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		topic := cmd.StringArg("topic")
		if topic == "" {
			return errors.New("topic must be provided")
		}

		group := cmd.StringArg("group")
		if group == "" {
			return errors.New("group must be provided")
		}

		partition := cmd.Uint32Arg("partition")
		offset := cmd.Uint64Arg("offset")

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err != client.ErrCloseConnection {
				fmt.Println("error", err)
			}
		})
		if err != nil {
			return err
		}

		resp, err := conn.CommitOffset(topic, group, partition, offset)
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
