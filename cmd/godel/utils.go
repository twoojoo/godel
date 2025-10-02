package main

import (
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/urfave/cli/v3"
)

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

func getGroupFromConsumerID(id string) (string, error) {
	parts := strings.Split(id, "-")
	if len(parts) < 2 {
		return "", errors.New("invalid format: missing '-' separator")
	}

	// Rejoin everything except the last part as substring
	substring := strings.Join(parts[:len(parts)-1], "-")
	idPart := parts[len(parts)-1]

	// Validate UUID
	_, err := uuid.Parse(idPart)
	if err != nil {
		return "", errors.New("invalid UUID part")
	}

	if substring == "" {
		return "", errors.New("substring cannot be empty")
	}

	return substring, nil
}
