package client

import (
	"context"
	"godel/broker"
	"godel/options"
	"os"
	"time"

	"github.com/urfave/cli/v3"
)

var CommandRunServer = &cli.Command{
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
		opts := options.DeafaultBrokerOptions()

		if configPath := cmd.String("config"); configPath != "" {
			opts, err = options.LoadBrokerOptionsFromYaml(configPath)
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
			return err
		}

		// error from this function is already logged
		gb.Run(port)
		os.Exit(1)
		return nil
	},
}
