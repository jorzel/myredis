package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type CommandParser interface {
	Parse(rawMessage []byte) (ParseResult, error)
}

type Command struct {
	Name string
	Args []string
}

func NewCommand(name string, args []string, bytesSize int) Command {
	return Command{
		Name: strings.ToUpper(name),
		Args: args,
	}
}

// ParseResult holds either parsed commands or an RDB payload.
type ParseResult struct {
	Commands []Command
}

func NewCommandParser() CommandParser {
	return DefaultCommandParser{}
}

// DefaultCommandParser parses raw RESP messages.
type DefaultCommandParser struct{}

// Parse parses rawMessage (RESP format) into commands or RDB payload.
func (p DefaultCommandParser) Parse(rawMessage []byte) (ParseResult, error) {
	result := ParseResult{}

	reader := bufio.NewReader(bytes.NewReader(rawMessage))

	for {
		b, err := reader.Peek(1)
		if err == io.EOF {
			break // no more messages
		}
		if err != nil {
			return result, err
		}

		switch b[0] {
		case '*':
			cmd, err := readCommand(reader)
			if err != nil {
				return result, err
			}
			result.Commands = append(result.Commands, cmd)

		default:
			return result, fmt.Errorf("unsupported RESP message start byte: %q", b[0])
		}
	}

	return result, nil
}

func readCommand(r *bufio.Reader) (Command, error) {
	bytesSize := 0
	line, err := readLine(r)
	bytesSize += len(line) + 2 // +2 for CRLF

	if err != nil {
		return Command{}, err
	}

	if len(line) == 0 || line[0] != '*' {
		return Command{}, fmt.Errorf("expected array, got: %q", line)
	}

	argCount, err := strconv.Atoi(line[1:])
	if err != nil {
		return Command{}, fmt.Errorf("invalid array length: %w", err)
	}

	args := make([]string, 0, argCount)
	for i := 0; i < argCount; i++ {
		arg, err := readBulkString(r)

		bytesSize += len(arg) + 2                          // +2 for CRLF
		bytesSize += len(fmt.Sprintf("$%d", len(arg))) + 2 // for the '$' prefix
		if err != nil {
			return Command{}, err
		}
		args = append(args, arg)
	}

	if len(args) == 0 {
		return Command{}, fmt.Errorf("empty command")
	}

	return NewCommand(args[0], args[1:], bytesSize), nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got: %q", line)
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk length: %w", err)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	crlf := make([]byte, 2)
	if _, err := io.ReadFull(r, crlf); err != nil {
		return "", err
	}
	if !bytes.Equal(crlf, []byte(CRLF)) {
		return "", fmt.Errorf("expected CRLF after bulk string, got: %q", crlf)
	}

	return string(buf), nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("protocol error: expected CRLF ending, got %q", line)
	}
	return line[:len(line)-2], nil // strip \r\n
}
