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
			commandRunServer,
			commandConsume,
			commandProduce,
			{
				Name: "topic",
				Commands: []*cli.Command{
					commandCreateTopic,
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
