package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"
	"godel/options"

	"github.com/urfave/cli/v3"
)

var commandCreateTopic = &cli.Command{
	Name: "add",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "name",
		},
	},
	Flags: []cli.Flag{
		&cli.Uint32Flag{
			Name:     "partitions",
			Aliases:  []string{"p"},
			Usage:    "number of partitions for the new topic",
			OnlyOnce: true,
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		name := cmd.StringArg("name")
		if name == "" {
			return errors.New("name is required")
		}

		conn, err := client.ConnectToBroker(getAddr(cmd))
		if err != nil {
			return err
		}

		topicOptions := options.TopicOptions{
			NumPartitions:   cmd.Uint32("partitions"),
			CleanupPolicy:   options.CleanupPolicy(cmd.String("cleanup")),
			RetentionMilli:  cmd.Int64("retention.ms"),
			RetentionBytes:  cmd.Int32("retention.bytes"),
			SegmentBytes:    cmd.Int32("segment.bytes"),
			MaxMessageBytes: cmd.Int32("max.message.bytes"),
		}

		resp, err := conn.CreateTopics(name, &topicOptions)
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
