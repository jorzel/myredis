package server

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/jorzel/myredis/app/commands"
	"github.com/jorzel/myredis/app/config"
	"github.com/jorzel/myredis/app/protocol"
	"github.com/rs/zerolog"
)

var _ Server = (*MasterServer)(nil)

type MasterServer struct {
	listener       net.Listener
	commandParser  protocol.CommandParser
	commandHandler commands.CommandHandler
	config         *config.Config
	role           string
}

func NewMasterServer(cfg *config.Config) (*MasterServer, error) {
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.ServerPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &MasterServer{
		listener:       ln,
		commandParser:  protocol.NewCommandParser(),
		commandHandler: commands.NewCommandHandler(cfg),
		config:         cfg,
		role:           config.MasterRole,
	}, nil
}

func (ms *MasterServer) Start(ctx context.Context) error {
	logger := zerolog.Ctx(ctx)
	logger.Info().
		Str("address", ms.listener.Addr().String()).
		Str("role", ms.role).
		Msg("Server listening on...")
	for {
		conn, err := ms.listener.Accept()
		if err != nil {
			continue
		}
		go ms.handleConnection(ctx, conn)
	}
}

func (ms *MasterServer) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	logger := zerolog.Ctx(ctx).With().
		Str("remote_addr", conn.RemoteAddr().String()).
		Logger()
	logger.Info().Msg("Handling new connection")

	buffer := make([]byte, 1024*4)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				logger.Info().Msg("Connection closed by client")
				return
			}
			logger.Err(err).Msg("Error reading from connection")
			return
		}

		logger.Debug().Str("data", string(buffer[:n])).Msg("Received data")
		result, err := ms.commandParser.Parse(buffer[:n])
		if err != nil {
			logger.Err(err).Msg("Failed to parse received data")
			continue
		}

		for _, command := range result.Commands {
			logger := logger.With().
				Str("command", command.Name).
				Interface("args", command.Args).Logger()
			logger.Info().Msg("Parsed command")

			result, err := ms.commandHandler.Handle(ctx, conn, command)
			if err != nil {
				logger.Err(err).Msg("Failed to handle command")
				continue
			}
			if result.CommandError != nil {
				logger.Err(result.CommandError).Msg("Command error occurred, sending error response")
			}
		}

	}
}
