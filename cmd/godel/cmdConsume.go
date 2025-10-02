package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"godel/internal/client"
	"godel/internal/protocol"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
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

var commandConsume = &cli.Command{
	Name: "consume",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "group",
			Aliases: []string{"g"},
		},
		&cli.BoolFlag{
			Name: "fromBeginning",
		},
		&cli.BoolFlag{
			Name: "json",
		},
		&cli.Int32Flag{
			Name:    "number",
			Aliases: []string{"n"},
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		topic := cmd.StringArg("topic")
		if topic == "" {
			return errors.New("topic must be provided")
		}

		group := cmd.String("group")

		maxMessages := cmd.Int32("number")

		consumerID := uuid.NewString()

		corrID, err := client.GenerateCorrelationID()
		if err != nil {
			return err
		}

		conn, err := client.ConnectToBroker(getAddr(cmd))
		if err != nil {
			return err
		}

		var alreadyClosed bool

		close := func() {
			if alreadyClosed {
				return
			}

			conn, err := client.ConnectToBroker(getAddr(cmd))
			if err != nil {
				fmt.Println(err)
			}

			if group == "" {
				consumerID = consumerID + "-" + consumerID
			} else {
				consumerID = group + "-" + consumerID
			}

			fmt.Println("disconnecting consumer...")
			resp, err := conn.DeleteConsumer(topic, group, consumerID)
			if err != nil {
				fmt.Println(err)
			}
			if resp.ErrorCode != 0 {
				fmt.Println("Unxexpected Error:", resp.ErrorMessage)
			}

			alreadyClosed = true
		}

		go func() {
			count := 0
			conn.ReadMessage(func(r *protocol.BaseResponse) error {
				if corrID != r.CorrelationID {
					return nil
				}

				resp, err := protocol.DeserializeResponseConsume(r.Payload)
				if err != nil {
					return err
				}

				for i := range resp.Messages {
					if count >= int(maxMessages) && maxMessages != 0 {
						close()
						os.Exit(0)
					}

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
			ID:            consumerID,
			Topic:         topic,
			Group:         group,
			FromBeginning: cmd.Bool("fromBeginning"),
		}

		reqBuf, err := req.Serialize()
		if err != nil {
			return err
		}

		msg := &protocol.BaseRequest{
			Cmd:           protocol.CmdConsume,
			ApiVersion:    0,
			CorrelationID: corrID,
			Payload:       reqBuf,
		}

		onShutdown(close)

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
