package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name: "godel",
		Commands: []*cli.Command{
			cmdRunServer,
			cmdConsume,
			cmdProduce,
			cmdCommitoffset,
			{
				Name: "topic",
				Commands: []*cli.Command{
					cmdCreateTopic,
				},
			},
			{
				Name: "consumer",
				Commands: []*cli.Command{
					cmdDeleteConsumer,
				},
			},
			{
				Name: "group",
				Commands: []*cli.Command{
					cmdListConsumerGroups,
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
