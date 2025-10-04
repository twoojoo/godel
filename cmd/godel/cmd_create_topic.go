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

var cmdCreateTopic = &cli.Command{
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
		&cli.Int64Flag{
			Name:  "heartbeat.interval.ms",
			Value: options.DefaultHeartbeatIntervalMs,
		},
		&cli.Int64Flag{
			Name:  "session.timeout.ms",
			Value: options.DefaultSessionTimeoutMs,
		},
		&cli.BoolFlag{
			Name:  "enable.auto.commit",
			Value: false,
		},
		&cli.Int64Flag{
			Name:  "auto.commit.interval.ms",
			Value: options.DefaultAutoCommitIntervalMs,
		},
		&cli.Int64Flag{
			Name:  "max.message.bytes",
			Value: options.DefaultMaxMessageBytes,
		},
		&cli.Int64Flag{
			Name:  "retention.ms",
			Value: options.DefaultRetentionMs,
		},
		&cli.Int64Flag{
			Name:  "retention.bytes",
			Value: options.DefaultRetentionBytes,
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		name := cmd.StringArg("name")
		if name == "" {
			return errors.New("name is required")
		}

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err != client.ErrCloseConnection {
				fmt.Println("error", err)
			}
		})
		if err != nil {
			return err
		}

		topicOptions := options.TopicOptions{
			NumPartitions:   cmd.Uint32("partitions"),
			CleanupPolicy:   options.CleanupPolicy(cmd.String("cleanup")),
			RetentionMilli:  cmd.Int64("retention.ms"),
			RetentionBytes:  cmd.Int64("retention.bytes"),
			SegmentBytes:    cmd.Int64("segment.bytes"),
			MaxMessageBytes: cmd.Int64("max.message.bytes"),
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
