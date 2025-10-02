package main

import (
	"context"
	"godel/internal/client"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name: "godel",
		Commands: []*cli.Command{
			client.CommandRunServer,
			client.CommandProduce,
			client.CommandConsume,
			{
				Name: "topic",
				Commands: []*cli.Command{
					client.CommandCreateTopic,
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
