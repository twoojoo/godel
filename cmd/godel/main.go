package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/urfave/cli/v3"
)

func main() {
	setLogger()

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
					cmdGetTopic,
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
					cmdGetConsumerGroup,
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

func setLogger() {
	debug := strings.ToLower((os.Getenv("DEBUG")))
	if debug == "1" || debug == "true" {
		handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})

		slog.SetDefault(slog.New(handler))
	}
}
