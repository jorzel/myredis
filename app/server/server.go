package server

import (
	"context"
	"net"
	"strconv"
	"time"

	"fmt"
	"io"
	"os"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/rs/zerolog"
)

type Server struct {
	Storage        sync.Map
	CommandHandler commands.CommandHandler
	CommandParser  protocol.CommandParser
	ClientFactory  replication.ClientFactory
	Config         *config.Config
}

func NewServer(config *config.Config) *Server {
	return &Server{
		CommandHandler: commands.NewCommandHandler(config),
		CommandParser:  protocol.NewCommandParser(),
		ClientFactory:  replication.NewClientFactory(),
		Config:         config,
	}
}

func (srv *Server) Init(ctx context.Context) error {
	logger := zerolog.Ctx(ctx).With().Timestamp().Logger()
	if srv.Config.IsSlave() {
		masterAddress := fmt.Sprintf("%s:%d", srv.Config.ReplicaOf.Host, srv.Config.ReplicaOf.Port)
		logger.Info().
			Str("replica_of", masterAddress).
			Msg("Server configured as slave")
		client, err := srv.handshake(ctx, masterAddress)
		if err != nil {
			return err
		}
		if client == nil {
			return fmt.Errorf("failed to establish connection with master server at %s", masterAddress)
		}
		defferedContext := context.WithoutCancel(ctx)
		// Start a goroutine to handle the connection to the master server
		// to be able to handle replication writes
		go srv.handleConnectionToMaster(defferedContext, client.Connection)
	} else {
		logger.Info().Msg("Server is configured as master")
	}
	return nil
}

func (srv *Server) handshake(ctx context.Context, address string) (*replication.Client, error) {
	logger := zerolog.Ctx(ctx).With().Timestamp().Logger()
	logger.Info().Msg("Performing handshake with the master server...")

	client, err := srv.ClientFactory.GetClient(address)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection for master server at %s: %w", address, err)
	}

	if err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to perform ping with the master server: %w", err)
	}

	if err := client.ReplConf(ctx, []string{"listening-port", strconv.Itoa(srv.Config.ServerPort)}); err != nil {
		return nil, fmt.Errorf("failed to perform REPLCONF listening-port with the master server: %w", err)
	}

	if err := client.ReplConf(ctx, []string{"capa", "psync2"}); err != nil {
		return nil, fmt.Errorf("failed to perform REPLCONF capa psync2 with the master server: %w", err)
	}

	if err := client.PSync(ctx, []string{"?", "-1"}); err != nil {
		return nil, fmt.Errorf("failed to perform PSYNC with the master server: %w", err)
	}

	logger.Info().Msg("Handshake successful")
	return client, nil
}

func (srv *Server) ListenAndServe(ctx context.Context, address string) error {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	logger.Info().Str("address", address).Msg("Starting listening on connections...")
	l, err := net.Listen("tcp", address)
	if err != nil {
		logger.Err(err).Str("address", address).Msg("Failed to bind to port")
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Err(err).Msg("Error accepting connection")
			continue
		}
		go srv.handleConnection(ctx, conn)
	}

}

func (srv *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	logger := zerolog.Ctx(ctx).With().Str("op", "handle_conn").Str("remote_addr", conn.RemoteAddr().String()).Logger()
	logger.Info().Msg("Handling new connection")

	buffer := make([]byte, 1024)

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
		cmds, err := srv.CommandParser.Parse(buffer[:n])
		if err != nil {
			logger.Err(err).Msg("Failed to parse received data")
			continue
		}

		for i, command := range cmds {
			logger.Info().Str("command", command.Name).Int("index", i).Msg("Parsed command")
			result, err := srv.CommandHandler.Handle(ctx, conn, command)
			if err != nil {
				logger.Err(err).Msg("Failed to handle command")
				continue
			}
			if result.CommandError != nil {
				logger.Err(result.CommandError).Msg("Command error occurred, sending error response")
			}
			if srv.shouldPropagateCommand(command) {
				logger := logger.With().Str("command", command.Name).Interface("args", command.Args).Logger()
				conns := srv.CommandHandler.GetSlaveConns(ctx)
				logger.Info().
					Int("slave_count", len(conns)).
					Msg("Write command executed, replicating to slaves")
				for i, conn := range conns {
					client := srv.ClientFactory.GetClientUsingConn(conn)
					if client == nil {
						logger.Error().Msg("Failed to get client for slave connection")
						continue
					}
					err := client.Propagate(ctx, command)
					logger.Info().
						Int("slave_index", i).
						Str("slave_address", conn.RemoteAddr().String()).
						Msg("Command propagated to slave")
					if err != nil {
						logger.Err(err).Msg("Failed to propagate command to slave")
						continue
					}
				}
			}
		}

	}
}

func (srv *Server) shouldPropagateCommand(command protocol.Command) bool {
	if srv.Config.IsSlave() {
		return false
	}
	return protocol.IsWriteCommand(command)
}

func (srv *Server) handleConnectionToMaster(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	logger := zerolog.Ctx(ctx).With().Str("op", "handle_propagation_conn").Logger()
	logger.Info().Msg("Handling new connection for propagation")

	buffer := make([]byte, 1024)

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				logger.Info().Msg("Replication connection alive, waiting for data...")
			case <-ctx.Done():
				return
			}
		}
	}()

	for {

		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				logger.Err(err).Msg("Connection closed by client")
				return
			}
			logger.Err(err).Msg("Error reading from connection")
			return
		}

		logger.Debug().Str("data", string(buffer[:n])).Msg("Received data for propagation")
		cmds, err := srv.CommandParser.Parse(buffer[:n])
		if err != nil {
			logger.Err(err).Msg("Failed to parse received data")
			continue
		}

		for i, command := range cmds {
			logger.Info().Str("command", command.Name).Int("index", i).Msg("Parsed command")
			if !protocol.IsWriteCommand(command) {
				logger.Warn().Str("command", command.Name).Msg("This is not a write command, skipping propagation")
				continue
			}
			_, err := srv.CommandHandler.Handle(ctx, conn, command)
			if err != nil {
				logger.Err(err).Msg("Failed to handle command")
				continue
			}
			logger.Info().Str("command", command.Name).Msg("Command handled successfully")
		}
	}
}
