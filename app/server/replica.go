package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/jorzel/myredis/app/commands"
	"github.com/jorzel/myredis/app/config"
	"github.com/jorzel/myredis/app/protocol"
	"github.com/rs/zerolog"
)

var _ Server = (*ReplicaServer)(nil)

type ReplicaServer struct {
	listener       net.Listener
	commandParser  protocol.CommandParser
	commandHandler commands.CommandHandler
	config         *config.Config
	role           string
}

func NewReplicaServer(cfg *config.Config) (*ReplicaServer, error) {
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.ServerPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &ReplicaServer{
		listener:       ln,
		commandParser:  protocol.NewCommandParser(),
		commandHandler: commands.NewCommandHandler(cfg),
		config:         cfg,
		role:           config.ReplicaRole,
	}, nil
}

func (rs *ReplicaServer) Start(ctx context.Context) error {
	logger := zerolog.Ctx(ctx)
	if err := rs.setup(ctx); err != nil {
		return fmt.Errorf("failed to setup replica server: %w", err)
	}
	logger.Info().
		Str("address", rs.listener.Addr().String()).
		Str("role", rs.role).
		Msg("Server listening on...")
	for {
		conn, err := rs.listener.Accept()
		if err != nil {
			continue
		}
		go rs.handleConnection(ctx, conn)
	}
}

func (rs *ReplicaServer) setup(ctx context.Context) error {
	logger := zerolog.Ctx(ctx)

	if rs.config.ReplicaOf == nil {
		return fmt.Errorf("replica server is not configured to replicate from a master server")
	}

	masterAddress := fmt.Sprintf("%s:%d", rs.config.ReplicaOf.Host, rs.config.ReplicaOf.Port)
	logger.Info().
		Str("replica_of", masterAddress).
		Msg("Server configured as replica of")
	err := rs.handshake(ctx, masterAddress)

	// Start a goroutine to handle the connection to the master server
	// to be able to handle replication writes
	return err
}

func (rs *ReplicaServer) handshake(ctx context.Context, address string) error {
	logger := zerolog.Ctx(ctx)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to server at %s: %w", address, err)
	}
	logger.Info().Msg("Connected to master server, starting handshake")
	reader := bufio.NewReader(conn)
	logger.Info().Str("remote_addr", conn.RemoteAddr().String()).Msg("Connected to master server")

	conn.Write(protocol.BulkArray([]string{"PING"}))
	if err := expectSimpleResponse(reader, "PONG"); err != nil {
		return fmt.Errorf("failed to perform ping with the master server: %w", err)
	}

	logger.Info().Msg("Ping successful, proceeding with REPLCONF handshake")
	conn.Write(protocol.BulkArray([]string{"REPLCONF", "listening-port", strconv.Itoa(rs.config.ServerPort)}))
	if err := expectSimpleResponse(reader, "OK"); err != nil {
		return fmt.Errorf("failed to perform REPLCONF listening-port with the master server: %w", err)
	}

	logger.Info().Msg("REPLCONF listening-port successful, proceeding with REPLCONF capa psync2")
	conn.Write(protocol.BulkArray([]string{"REPLCONF", "capa", "psync2"}))
	if err := expectSimpleResponse(reader, "OK"); err != nil {
		return fmt.Errorf("failed to perform REPLCONF capa psync2 with the master server: %w", err)
	}

	logger.Info().Msg("REPLCONF capa psync2 successful, proceeding with PSYNC handshake")
	conn.Write(protocol.BulkArray([]string{"PSYNC", "?", "-1"}))

	logger.Info().Msg("Handshake initiated successfully")
	defferedContext := context.WithoutCancel(ctx)
	go rs.handleReplicationConnection(defferedContext, conn)
	return nil
}

func expectSimpleResponse(reader *bufio.Reader, expected string) error {
	resp, err := protocol.ParseResponse(reader)
	if err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}
	if resp.Type != protocol.SimpleStringType {
		return fmt.Errorf("unexpected response type: %s, expected: %s", resp.Type, protocol.SimpleStringType)
	}
	if resp.Value != expected {
		return fmt.Errorf("unexpected response: %s", resp.Value)
	}
	return nil
}

func (rs *ReplicaServer) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	logger := zerolog.Ctx(ctx).With().
		Str("role", rs.role).
		Str("op", "handle_conn").
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
		result, err := rs.commandParser.Parse(buffer[:n])
		if err != nil {
			logger.Err(err).Msg("Failed to parse received data")
			continue
		}

		for i, command := range result.Commands {
			logger := logger.With().
				Str("command", command.Name).
				Interface("args", command.Args).Logger()
			logger.Info().Int("index", i).Msg("Parsed command")
			result, err := rs.commandHandler.Handle(ctx, conn, command)
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

func (rs *ReplicaServer) handleReplicationConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	logger := zerolog.Ctx(ctx).With().
		Str("role", rs.role).
		Str("local_addr", conn.LocalAddr().String()).
		Str("op", "handle_replication_conn").Logger()
	logger.Info().Msg("Handling new connection for replication")

	buffer := make([]byte, 1024*4)

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

		logger.Debug().Str("data", string(buffer[:n])).Msg("Received data for replication")
		result, err := rs.commandParser.Parse(buffer[:n])
		if err != nil {
			logger.Err(err).Msg("Failed to parse received data")
			continue
		}

		if result.RDBDump != nil {
			logger.Info().Msg("Received RDB dump from master server")
		}
		for i, command := range result.Commands {
			logger := logger.With().
				Str("command", command.Name).
				Interface("args", command.Args).Logger()
			logger.Info().Int("index", i).Msg("Parsed command")
			_, err := rs.commandHandler.Handle(ctx, conn, command)
			if err != nil {
				logger.Err(err).Msg("Failed to handle replicated command")
				continue
			}
		}
	}
}
