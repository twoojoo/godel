package main

import (
	"context"
	"godel/broker"
	"log"
	"os"
	"time"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name: "server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Aliases:  []string{"c"},
				Usage:    "path for the configuratio file",
				OnlyOnce: true,
			},
			&cli.IntFlag{
				Name:     "port",
				Usage:    "port the server will be listening at",
				OnlyOnce: true,
			},
			&cli.Int32Flag{
				Name:     "base.path",
				Usage:    "base path for the godel broker",
				OnlyOnce: true,
			},
			&cli.Int64Flag{
				Name:     "log.retention.check.interval.ms",
				Aliases:  []string{"p"},
				Usage:    "interval at which the retention check will be scheduled by the broker",
				OnlyOnce: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) (err error) {
			opts := broker.DeafaultBrokerOptions()

			if configPath := cmd.String("config"); configPath != "" {
				opts, err = broker.LoadBrokerOptionsFromYaml(configPath)
				if err != nil {
					return err
				}
			}

			if basePath := cmd.String("base.path"); basePath != "" {
				opts.WithBasePath(basePath)
			}

			if lrcims := cmd.Int64("log.retention.check.interval.ms"); lrcims != 0 {
				opts.WithLogRetentionCheckInterval(time.Duration(lrcims) * time.Millisecond)
			}

			port := cmd.Int("port")
			if port == 0 {
				port = 9090
			}

			gb, err := broker.NewBroker(opts)
			if err != nil {
				return nil
			}

			gb.Run(port)

			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}

	// options := broker.DeafaultBrokerOptions().
	// 	WithBasePath("./test").
	// 	WithLogRetentionCheckInterval(time.Minute * 5)

	// gb, err := broker.NewBroker(options)
	// if err != nil {
	// 	panic(err)
	// }

	// topicOptions := broker.DefaultTopicOptions().
	// 	WithRetentionMilli(time.Minute * 5)

	// topic, err := gb.GetOrCreateTopic("mytopic", topicOptions)
	// if err != nil {
	// 	panic(err)
	// }

	// go func() {
	// 	time.Sleep(2 * time.Second)

	// 	err = topic.Consume(5, func(m *broker.Message) error {
	// 		slog.Info("message", "offset", m.Offset(), "key", m.Key(), "payload", m.Payload())
	// 		time.Sleep(time.Millisecond * 500)
	// 		return nil
	// 	})
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()

	// for i := 0; i < 10; i++ {
	// 	offset, err := gb.Produce("mytopic", broker.NewMessage(
	// 		uint64(time.Now().Unix()),
	// 		[]byte("key"+strconv.Itoa(i)),
	// 		[]byte("abracadabra"),
	// 	))
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	fmt.Println("offset", offset)
	// }

	// select {}
}
