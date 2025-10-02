package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"godel/internal/client"
	"godel/internal/protocol"
	"os"
	"strings"

	"github.com/urfave/cli/v3"
)

var cmdProduce = &cli.Command{
	Name: "produce",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "topic",
		},
		&cli.StringArg{
			Name: "key",
		},
	},
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:     "showResponse",
			Aliases:  []string{"s"},
			OnlyOnce: true,
		},
	},
	Action: func(ctx context.Context, cmd *cli.Command) (err error) {
		topic := cmd.StringArg("topic")
		key := []byte(cmd.StringArg("key"))

		if topic == "" {
			return errors.New("topic must be provided")
		}

		corrID, err := client.GenerateCorrelationID()
		if err != nil {
			return err
		}

		conn, err := client.ConnectToBroker(getAddr(cmd))
		if err != nil {
			return err
		}

		if cmd.Bool("showResponse") {
			go func() {
				err := conn.ReadMessage(func(r *protocol.BaseResponse) error {
					if corrID != r.CorrelationID {
						return nil
					}

					fmt.Println(string(r.Payload))

					_, err := protocol.DeserializeResponseProduce(r.Payload)
					if err != nil {
						return err
					}

					return nil
				})
				if err != nil {
					fmt.Println("error", err)
				}
			}()
		}

		for {
			reader := bufio.NewReader(os.Stdin)
			print(" > ")
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("error", err)
			}

			input = strings.TrimSuffix(input, "\n")

			req := protocol.ReqProduce{
				Topic: topic,
				Messages: []protocol.ReqProduceMessage{
					{
						Key:   key,
						Value: []byte(input),
					},
				},
			}

			reqBuf, err := req.Serialize()
			if err != nil {
				return err
			}

			msg := &protocol.BaseRequest{
				Cmd:           protocol.CmdProduce,
				ApiVersion:    0,
				CorrelationID: corrID,
				Payload:       reqBuf,
			}

			err = conn.SendMessage(msg)
			if err != nil {
				fmt.Println(err)
			}
		}
	},
}
