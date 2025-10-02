package client

import (
	"context"
	"fmt"
	"godel/internal/protocol"
	"godel/options"
	"os"

	"github.com/urfave/cli/v3"
)

var CommandCreateTopic = &cli.Command{
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
		corrID, err := generateCorrelationID()
		if err != nil {
			return err
		}

		conn, err := connectToBroker(cmd)
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

		options.MergeTopicOptions(&topicOptions, options.DefaultTopicOptions())

		req := protocol.ReqCreateTopic{
			Topics: []protocol.ReqCreateTopicTopic{
				{
					Name:    cmd.StringArg("name"),
					Configs: topicOptions,
				},
			},
		}

		reqBuf, err := req.Serialize()
		if err != nil {
			return err
		}

		msg := &protocol.BaseRequest{
			Cmd:           protocol.CreateTopics,
			ApiVersion:    0,
			CorrelationID: corrID,
			Payload:       reqBuf,
		}

		go func() {
			err = conn.sendMessage(msg)
			if err != nil {
				fmt.Println(err)
			}
		}()

		return conn.readMessage(func(r *protocol.BaseResponse) error {
			if msg.CorrelationID != r.CorrelationID {
				return nil
			}

			fmt.Println(string(r.Payload))

			_, err := protocol.DeserializeResponseCreateTopic(r.Payload)
			if err != nil {
				return err
			}

			os.Exit(0)
			return nil
		})
	},
}
