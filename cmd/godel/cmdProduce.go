package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"godel/internal/client"
	"godel/internal/protocol"
	"io"
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

		// closeCh := make(chan struct{})

		conn, err := client.ConnectToBroker(getAddr(cmd), func(c *client.Connection, err error) {
			if err == client.ErrCloseConnection || err == io.EOF {
				fmt.Fprintf(os.Stderr, "\n")
				fmt.Fprintf(os.Stderr, "producer forcefully disconnected\n")

				if err := c.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
				}

				os.Exit(0)
			}

			fmt.Println("error", err)
		})
		if err != nil {
			return err
		}

		if cmd.Bool("showResponse") {
			go func() {
				err := conn.ReadMessage(corrID, func(r *protocol.BaseResponse) error {
					fmt.Println(string(r.Payload))

					_, err := protocol.DeserializeResponseProduce(r.Payload)
					if err != nil {
						return err
					}

					return nil
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
				}
			}()
		}

		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Fprint(os.Stderr, " > ")
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
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
				fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			}
		}
	},
}
