package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jorzel/myredis/app/config"
	"github.com/jorzel/myredis/app/server"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	ctx := logger.WithContext(context.Background())

	cfg, err := getInitSpecsFromArgs()
	if err != nil {
		logger.Err(err).Msg("Failed to parse initialization specifications from arguments")
		os.Exit(1)
	}

	logger.Info().Interface("config", cfg).Msg("Starting configuration")

	var srv server.Server
	if cfg.ReplicaOf != nil {
		srv, err = server.NewMasterServer(cfg)
	} else {
		srv, err = server.NewReplicaServer(cfg)
	}

	if err != nil {
		logger.Err(err).Msg("Failed to initialize server")
		os.Exit(1)
	}
	err = srv.Start(ctx)
	if err != nil {
		logger.Err(err).Msg("Failed to start server")
		os.Exit(1)
	}
}

func getInitSpecsFromArgs() (*config.Config, error) {
	port := flag.Int("port", 6379, "Port to listen on")
	replicaOf := flag.String("replicaof", "", "Address of the master server")
	flag.Parse()

	if port == nil {
		return nil, fmt.Errorf("port flag is required")
	}
	if *port < 1 || *port > 65535 {
		return nil, fmt.Errorf("port must be between 1 and 65535, got %d", *port)
	}

	var err error
	var deserializedReplicaOf *config.Node
	if replicaOf == nil || *replicaOf == "" {
		deserializedReplicaOf = nil
	} else {
		deserializedReplicaOf, err = validateReplicaOf(*replicaOf)
		if err != nil {
			return nil, fmt.Errorf("invalid replicaof address: %w", err)
		}
	}

	return &config.Config{
		ReplicaOf:  deserializedReplicaOf,
		ServerPort: *port,
	}, nil
}

func validateReplicaOf(replicaOf string) (*config.Node, error) {
	if replicaOf == "" {
		return nil, nil
	}

	parts := strings.Split(replicaOf, " ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("replicaof must be in the format <host>:<port>, got %s", replicaOf)
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port number in replicaof: %s", parts[1])
	}

	return &config.Node{
		Host: parts[0],
		Port: port,
	}, nil
}
