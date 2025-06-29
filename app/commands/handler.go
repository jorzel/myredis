package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jorzel/myredis/app/config"
	"github.com/jorzel/myredis/app/protocol"
	"github.com/jorzel/myredis/app/storage"
	"github.com/rs/zerolog"
)

type HandleResult struct {
	CommandError error
}

type CommandHandler interface {
	Handle(ctx context.Context, conn net.Conn, command protocol.Command) (HandleResult, error)
}

var _ CommandHandler = (*DefaultCommandHandler)(nil)

type DefaultCommandHandler struct {
	storage storage.Storage
	config  *config.Config
}

// NewCommandHandler creates a new CommandHandler with an empty storage.
func NewCommandHandler(config *config.Config) CommandHandler {
	return &DefaultCommandHandler{
		config:  config,
		storage: storage.NewStorage(),
	}
}

func (h *DefaultCommandHandler) Handle(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	switch command.Name {
	case protocol.PING:
		return h.handlePing(ctx, conn, command)
	case protocol.SET:
		return h.handleSet(ctx, conn, command)
	case protocol.GET:
		return h.handleGet(ctx, conn, command)
	case protocol.DEL:
		return h.handleDel(ctx, conn, command) // DEL is not implemented
	case protocol.REPLCONF:
		return h.handleReplConf(ctx, conn, command)
	case protocol.PSYNC:
		return h.handlePsync(ctx, conn, command)
	case protocol.FULLRESYNC:
		return h.handleFullresync(ctx, conn, command)
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
	return protocol.SimpleString("PONG"), nil
}

func (h *DefaultCommandHandler) handleSet(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeSet(ctx, command)
	var err error
	err = h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeSet(_ context.Context, command protocol.Command) ([]byte, error) {
	if len(command.Args) < 2 {
		errMsg := "SET command requires at least 2 arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	record := storage.KVRecord{
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

	h.storage.Set(command.Args[0], &record)
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
	deserializedRecord, err := h.storage.Get(command.Args[0])
	if err != nil {
		errMsg := "Failed to get record: " + err.Error()
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}
	if deserializedRecord == nil {
		return protocol.Nil(), nil
	}
	if deserializedRecord.ExpireAt != nil && deserializedRecord.ExpireAt.Before(time.Now()) {
		// If the record has expired, return nil
		return protocol.Nil(), nil
	}
	return protocol.BulkString(deserializedRecord.Value), nil
}

func (h *DefaultCommandHandler) handleDel(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	msg, commandErr := h.executeDel(ctx, command)
	err := h.sendMsg(ctx, conn, msg)
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeDel(_ context.Context, command protocol.Command) ([]byte, error) {
	count := 0
	for i := 0; i < len(command.Args); i++ {
		err := h.storage.Del(command.Args[i])
		if err != nil {
			continue
		}
		count++
	}
	return protocol.SimpleInteger(count), nil
}

func (h *DefaultCommandHandler) handleReplConf(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	var err error
	msg, commandErr := h.executeReplConf(ctx, conn, command)
	if msg != nil {
		err = h.sendMsg(ctx, conn, msg)
	}
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executeReplConf(
	ctx context.Context, conn net.Conn, command protocol.Command,
) ([]byte, error) {
	if len(command.Args) != 2 {
		errMsg := "REPLCONF command requires exactly 2 arguments"
		return protocol.Error(errMsg), fmt.Errorf(errMsg)
	}

	if strings.ToLower(command.Args[0]) == "listening-port" {
		return protocol.SimpleString("OK"), nil
	} else if strings.ToLower(command.Args[0]) == "capa" && strings.ToLower(command.Args[1]) == "psync2" {
		return protocol.SimpleString("OK"), nil
	}
	errMsg := fmt.Sprintf(
		"REPLCONF command only supports 'listening-port <PORT>', and 'capa psync2' arguments, got: %s",
		strings.Join(command.Args, ", "),
	)
	return protocol.Error(errMsg), fmt.Errorf(errMsg)
}

func (h *DefaultCommandHandler) handlePsync(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	logger := zerolog.Ctx(ctx)

	msg, commandErr := h.executePsync(ctx, conn, command)
	err := h.sendMsg(ctx, conn, msg)
	if err != nil || commandErr != nil {
		return HandleResult{
			CommandError: commandErr,
		}, err
	}
	//time.Sleep(100 * time.Millisecond) // Simulate some delay
	msg, commandErr = h.getDBFile(ctx)

	err = h.sendMsg(ctx, conn, msg)
	logger.Info().Msg("Sending DB file to replica")
	return HandleResult{
		CommandError: commandErr,
	}, err
}

func (h *DefaultCommandHandler) executePsync(
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
	replID := "test"

	return protocol.SimpleString("FULLRESYNC " + replID + " 0"), nil
}

func (h *DefaultCommandHandler) getDBFile(_ context.Context) ([]byte, error) {
	// hex-encoded representation of a empty db file
	emptyDBFile := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	DBContent, _ := hex.DecodeString(emptyDBFile)
	return protocol.FileContent(DBContent), nil
}

func (h *DefaultCommandHandler) handleFullresync(
	ctx context.Context, conn net.Conn, command protocol.Command,
) (HandleResult, error) {
	logger := zerolog.Ctx(ctx)
	replID := command.Args[0]
	offset, err := strconv.Atoi(command.Args[1])
	if err != nil {
		errMsg := "Invalid offset in FULLRESYNC command: " + err.Error()
		return HandleResult{
			CommandError: fmt.Errorf(errMsg),
		}, fmt.Errorf(errMsg)
	}
	logger.Info().
		Str("command", command.Name).
		Int("offset", offset).
		Str("repl_id", replID).
		Msg("Handling FULLRESYNC command")
	return HandleResult{}, err
}
