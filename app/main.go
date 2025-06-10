package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/jorzel/myredis/app/config"
	"github.com/jorzel/myredis/app/server"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	ctx := logger.WithContext(context.Background())

	initSpec, err := getInitSpecsFromArgs()
	if err != nil {
		logger.Err(err).Msg("Failed to parse initialization specifications from arguments")
		os.Exit(1)
	}

	config, err := config.NewConfig(initSpec)
	if err != nil {
		logger.Err(err).Msg("Failed to create configuration")
		os.Exit(1)
	}
	logger.Info().Interface("config", config).Msg("Starting configuration")

	srv, err := server.NewMasterServer(config)

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

func getInitSpecsFromArgs() (config.InitSpec, error) {
	port := flag.Int("port", 6379, "Port to listen on")
	flag.Parse()

	if port == nil {
		return config.InitSpec{}, fmt.Errorf("port flag is required")
	}
	if *port < 1 || *port > 65535 {
		return config.InitSpec{}, fmt.Errorf("port must be between 1 and 65535, got %d", *port)
	}

	flag.Parse()

	return config.InitSpec{
		ServerPort: *port,
	}, nil
}
