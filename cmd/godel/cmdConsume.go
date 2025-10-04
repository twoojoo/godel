package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"
	"godel/internal/protocol"
	"godel/options"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"
)

type printableMessage struct {
	Key          string  `json:"key"`
	Partition    *uint32 `json:"partition,omitempty"`
	Offset       *uint64 `json:"offset,omitempty"`
	Payload      string  `json:"payload"`
	ErrorCode    int     `json:"errorCode"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

var cmdConsume = &cli.Command{
	Name: "consume",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
		&cli.StringArg{
			Name: "group",
		},
	},
	// 	func DefaulcConsumerOption() *ConsumerOptions {
	// 	return &ConsumerOptions{
	// 		SessionTimeoutMilli:     10000, // 10 secs
	// 		HeartbeatIntervalMilli:  3000,  // 3 secs
	// 		AutoCommitIntervalMilli: 5000,
	// 		EnableAutoCommit:        true,
	// 		FromBeginning:           false,
	// 	}
	// }
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "consumer",
			Aliases: []string{"c"},
			Usage:   "provide a consumer id to consume with (if not specified creates a new consumer in the consuer group)",
		},
		&cli.BoolFlag{
			Name:  "from.beginning",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "json",
			Value: false,
		},
		&cli.Int32Flag{
			Name:    "number",
			Aliases: []string{"n"},
			Value:   0,
		},
		&cli.Int64Flag{
			Name:  "heartbeat.interval.ms",
			Value: 3000,
		},
		&cli.Int64Flag{
			Name:  "session.timeout.ms",
			Value: 10000,
		},
		&cli.BoolFlag{
			Name:  "enable.auto.commit",
			Value: false,
		},
		&cli.Int64Flag{
			Name:  "auto.commit.interval.ms",
			Value: 5000,
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

		consumerID := cmd.String("consumer")
		maxMessages := cmd.Int32("number")

		corrID, err := client.GenerateCorrelationID()
		if err != nil {
			return err
		}

		opts := options.ConsumerOptions{
			SessionTimeoutMilli:     cmd.Int64("session.timeout.ms"),
			HeartbeatIntervalMilli:  cmd.Int64("heartbeat.interval.ms"),
			AutoCommitIntervalMilli: cmd.Int64("auto.commit.interval.ms"),
			EnableAutoCommit:        cmd.Bool("enable.auto.commit"),
		}

		options.MergeConsumerOptions(&opts, options.DefaulcConsumerOption())

		var alreadyClosed bool
		close := func(conn *client.Connection) {
			if alreadyClosed {
				os.Exit(0)
				return
			}

			// close anyway
			go func() {
				time.Sleep(500 * time.Millisecond)
				os.Exit(0)
			}()

			if consumerID != "" {
				fmt.Println("disconnecting consumer...")
				resp, err := conn.DeleteConsumer(topic, group, consumerID)
				if err != nil {
					fmt.Println("error deleting consumer", err)
				}
				if resp.ErrorCode != 0 {
					fmt.Println("Unxexpected Error:", resp.ErrorMessage)
				}
			}

			alreadyClosed = true
			os.Exit(0)
		}

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err != client.ErrCloseConnection {
				println("error", err)
				close(c)
				return
			}
		})
		if err != nil {
			return err
		}

		if consumerID == "" {
			consumerResp, err := conn.CreateConsumer(topic, group, &opts)
			if err != nil {
				return err
			}
			consumerID = consumerResp.ID
		}

		go func() {
			for {
				time.Sleep(time.Duration(opts.HeartbeatIntervalMilli) * time.Millisecond)
				_, err := conn.Heartbeat(topic, group, consumerID)
				if err != nil {
					println("heartbeat error", err)
				}
			}
		}()

		var latestOffsets = map[uint32]uint64{}

		if opts.EnableAutoCommit {
			go func() {
				for {
					time.Sleep(time.Duration(opts.AutoCommitIntervalMilli) * time.Millisecond)

					for partition, offset := range latestOffsets {
						_, err := conn.CommitOffset(topic, group, partition, offset)
						if err != nil {
							println("heartbeat error", err)
						}
					}
				}
			}()
		}

		go func() {
			count := 0
			conn.ReadMessage(corrID, func(r *protocol.BaseResponse) error {
				resp, err := protocol.Deserialize[protocol.RespConsume](r.Payload)
				if err != nil {
					return err
				}

				for i := range resp.Messages {
					if count >= int(maxMessages) && maxMessages != 0 {
						close(conn)
						os.Exit(0)
					}

					partition := *resp.Messages[i].Partition
					latestOffsets[partition] = *resp.Messages[i].Offset

					count++

					if cmd.Bool("json") {
						m := printableMessage{
							Key:          string(resp.Messages[i].Key),
							Partition:    resp.Messages[i].Partition,
							Offset:       resp.Messages[i].Offset,
							Payload:      string(resp.Messages[i].Payload),
							ErrorCode:    resp.Messages[i].ErrorCode,
							ErrorMessage: resp.Messages[i].ErrorMessage,
						}

						bytes, err := json.Marshal(&m)
						if err != nil {
							return err
						}

						fmt.Println(string(bytes))
						continue
					}

					fmt.Println(
						"key:", string(resp.Messages[i].Key),
						"partition:", *resp.Messages[i].Partition,
						"offset", *resp.Messages[i].Offset,
					)
					fmt.Println("payload", string(resp.Messages[i].Payload))
					fmt.Println()
				}

				return nil
			})
		}()

		req := protocol.ReqConsume{
			ID:              consumerID,
			Topic:           topic,
			Group:           group,
			FromBeginning:   cmd.Bool("fromBeginning"),
			ConsumerOptions: opts,
		}

		reqBuf, err := protocol.Serialize(req)
		if err != nil {
			return err
		}

		msg := &protocol.BaseRequest{
			Cmd:           protocol.CmdConsume,
			ApiVersion:    0,
			CorrelationID: corrID,
			Payload:       reqBuf,
		}

		onShutdown(func() {
			close(conn)
		})

		err = conn.SendMessage(msg)
		if err != nil {
			fmt.Println(err)
		}

		select {}
	},
}

func onShutdown(callback func()) {
	sigs := make(chan os.Signal, 1)

	// Catch common termination signals
	signal.Notify(sigs,
		syscall.SIGINT,  // Ctrl+C
		syscall.SIGTERM, // kill <pid>
		syscall.SIGHUP,  // terminal closed
		syscall.SIGQUIT, // Ctrl+\
	)

	go func() {
		<-sigs
		callback()
		os.Exit(0)
	}()
}
