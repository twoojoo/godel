package main

import "github.com/urfave/cli/v3"

func getAddr(cmd *cli.Command) string {
	host := "localhost"
	port := "9090"

	if hostOpt := cmd.String("host"); hostOpt != "" {
		host = hostOpt
	}

	if portOpt := cmd.String("port"); portOpt != "" {
		port = portOpt
	}

	return host + ":" + port
}
