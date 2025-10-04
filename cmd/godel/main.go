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
				Name:    "topic",
				Aliases: []string{"topics"},
				Commands: []*cli.Command{
					cmdCreateTopic,
					cmdListTopics,
					cmdDeleteTopic,
				},
			},
			{
				Name:    "consumer",
				Aliases: []string{"consumers"},
				Commands: []*cli.Command{
					cmdDeleteConsumer,
				},
			},
			{
				Name:    "group",
				Aliases: []string{"groups", "consumer-group", "consumer-groups"},
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
