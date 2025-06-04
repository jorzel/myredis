package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/rs/zerolog"
)

type HandleResult struct {
	CommandError error
}

type KeyValue struct {
	Value    string
	ExpireAt *time.Time
}

type CommandHandler interface {
	Handle(ctx context.Context, conn net.Conn, command protocol.Command) (HandleResult, error)
	GetSlaveConns(ctx context.Context) []net.Conn
}

var _ CommandHandler = (*DefaultCommandHandler)(nil)

type DefaultCommandHandler struct {
	storage    sync.Map
	config     *config.Config
	slaveConns ConnectionPool
}

// NewCommandHandler creates a new CommandHandler with an empty storage.
func NewCommandHandler(config *config.Config) CommandHandler {
	return &DefaultCommandHandler{
		config:     config,
		slaveConns: NewConnectionPool(),
	}
}

func (h *DefaultCommandHandler) GetSlaveConns(ctx context.Context) []net.Conn {
	if h.config.IsSlave() {
		return []net.Conn{}
	}
	if h.slaveConns == nil {
		return []net.Conn{}
	}
	return h.slaveConns.GetAll()
}

func (h *DefaultCommandHandler) Handle(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	switch command.Name {
	case protocol.PING:
		return h.handlePing(ctx, conn, command)
	case protocol.ECHO:
		return h.handleEcho(ctx, conn, command)
	case protocol.SET:
		return h.handleSet(ctx, conn, command)
	case protocol.GET:
		return h.handleGet(ctx, conn, command)
	case protocol.INFO:
		return h.handleInfo(ctx, conn, command)
	case protocol.REPLCONF:
		return h.handleReplConf(ctx, conn, command)
	case protocol.PSYNC:
		return h.handlePSync(ctx, conn, command)
	default:
		return h.handleUnknownCommand(ctx, conn, command)
	}
}

func (h *DefaultCommandHandler) sendMsg(_ context.Context, conn net.Conn, msg []byte) error {
	if _, err := conn.Write(msg); err != nil {
		return fmt.Errorf("Failed to write response: " + err.Error())
	}
	return nil
}

func (h *DefaultCommandHandler) handleUnknownCommand(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeUnknownCommand(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeUnknownCommand(
	_ context.Context, command protocol.Command,
) ([]byte, error) {
	errMsg := fmt.Sprintf("Unknown command: %s", command.Name)
	return protocol.Error(errMsg), fmt.Errorf(errMsg)
}

func (h *DefaultCommandHandler) handlePing(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executePing(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executePing(_ context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) != 0 {
		errorMsg := "PING command does not require any arguments"
		return protocol.Error(errorMsg), fmt.Errorf(errorMsg)
	}
	return protocol.SimpleString("PONG"), nil
}

func (h *DefaultCommandHandler) handleEcho(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeEcho(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeEcho(_ context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) != 1 {
		errorMsg := "ECHO command requires exactly 1 argument"
		return protocol.Error(errorMsg), fmt.Errorf(errorMsg)
	}
	return protocol.BulkString(strings.Join(command.Args, protocol.CRLF)), nil
}

func (h *DefaultCommandHandler) handleSet(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeSet(ctx, command)
	var err error
	if !h.config.IsSlave() {
		// If this is a master, we should send a response to the client
		// for slave connections, we don't send a response
		err = h.sendMsg(ctx, conn, msg)
	}
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeSet(_ context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) < 2 {
		errMsg := "SET command requires at least 2 arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	record := KeyValue{
		Value: command.Args[1],
	}
	if len(command.Args) > 2 {
		// If an expiration time is provided, parse it
		if strings.ToLower(command.Args[2]) == "px" {
			if len(command.Args) < 4 {
				errMsg := "SET command with PX requires 4 arguments"
				return protocol.Error(errMsg), fmt.Errorf(errMsg)
			}
			expiration, err := time.ParseDuration(command.Args[3] + "ms")
			if err != nil {
				errMsg := "Invalid expiration time: " + err.Error()
				return protocol.Error(errMsg), fmt.Errorf(errMsg)
			}
			expireAt := time.Now().Add(expiration)
			record.ExpireAt = &expireAt
		} else {
			errMsg := "Unsupported expiration format, only PX is supported"
			return protocol.Error(errMsg), fmt.Errorf(errMsg)
		}
	}

	h.storage.Store(command.Args[0], record)
	return protocol.SimpleString("OK"), nil
}

func (h *DefaultCommandHandler) handleGet(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeGet(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeGet(_ context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) != 1 {
		errMsg := "GET command requires exactly 1 argument"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	record, ok := h.storage.Load(command.Args[0])
	if !ok {
		return protocol.Nil(), nil
	}
	deserializedRecord, ok := record.(KeyValue)
	if !ok {
		errMsg := "Invalid record type"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	if deserializedRecord.ExpireAt != nil && deserializedRecord.ExpireAt.Before(time.Now()) {
		// If the record has expired, return nil
		return protocol.Nil(), nil
	}
	return protocol.BulkString(deserializedRecord.Value), nil
}

func (h *DefaultCommandHandler) handleInfo(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeInfo(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeInfo(_ context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) > 1 {
		errMsg := "INFO command too many arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}

	if len(command.Args) == 1 && strings.ToLower(command.Args[0]) != "replication" {
		errMsg := "INFO command only supports 'replication' argument"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}

	return protocol.BulkString(h.infoReplication()), nil
}

func (h *DefaultCommandHandler) handleReplConf(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeReplConf(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeReplConf(ctx context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) != 2 {
		errMsg := "REPLCONF command requires exactly 2 arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	if strings.ToLower(command.Args[0]) == "listening-port" {
		return protocol.SimpleString("OK"), nil
	} else if strings.ToLower(command.Args[0]) == "capa" && strings.ToLower(command.Args[1]) == "psync2" {
		return protocol.SimpleString("OK"), nil
	}
	errMsg := "REPLCONF command only supports 'listening-port <PORT>' and 'capa psync2' arguments"
	return protocol.Error(errMsg), fmt.Errorf(errMsg)
}

func (h *DefaultCommandHandler) handlePSync(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	logger := zerolog.Ctx(ctx)

	msg, commandErr := h.executePSync(ctx, conn, command)
	err := h.sendMsg(ctx, conn, msg)
	if err != nil || commandErr != nil {
		return HandleResult{
			CommandError: commandErr,
		}, err
	}
	msg, commandErr = h.getDBFile(ctx)

	if commandErr == nil {
		if conn == nil {
			logger.Error().Msg("Connection is nil, cannot add to slave connections")
			return HandleResult{
				CommandError: nil,
			}, fmt.Errorf("connection is nil")
		}
		logger.Info().Str("slave_addr", conn.RemoteAddr().String()).Msg("Adding slave connection")
		err = h.slaveConns.Add(conn)
		if err != nil {
			return HandleResult{
				CommandError: commandErr,
			}, fmt.Errorf("failed to add slave connection: %w", err)
		}
	}

	err = h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executePSync(
	ctx context.Context, conn net.Conn, command protocol.Command,
) ([]byte, error) {
	if len(command.Args) != 2 {
		errMsg := "PSYNC command requires exactly 2 arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	if command.Args[0] != "?" || command.Args[1] != "-1" {
		errMsg := "PSYNC command only supports '?' and '-1' arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}

	if !h.config.IsSlave() {
		return protocol.SimpleString("FULLRESYNC " + h.config.ReplicationID + " 0"), nil
	}
	return protocol.SimpleString("OK"), nil
}

func (h *DefaultCommandHandler) getDBFile(_ context.Context) ([]byte, error) {
	DBContent, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	return protocol.FileContent(DBContent), nil
}

func (h *DefaultCommandHandler) infoReplication() string {
	params := map[string]string{}
	params["role"] = "slave"
	if !h.config.IsSlave() {
		params["role"] = "master"
		params["master_repl_offset"] = "0"
		params["master_replid"] = h.config.ReplicationID
	}

	infoString := "#Replication"
	for key, value := range params {
		infoString += "\r\n" + key + ":" + value
	}
	return infoString
}
