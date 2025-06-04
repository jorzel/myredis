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
	Parse(rawMessage []byte) ([]Command, error)
}

var _ CommandParser = DefaultCommandParser{}

type DefaultCommandParser struct{}

func NewCommandParser() CommandParser {
	return DefaultCommandParser{}
}

type Command struct {
	Elements int
	Name     string
	Args     []string
}

func (p DefaultCommandParser) Parse(rawMessage []byte) ([]Command, error) {
	var commands []Command
	reader := bufio.NewReader(bytes.NewReader(rawMessage))

	for {
		// Expect array start
		prefix, err := reader.ReadByte()
		if err == io.EOF {
			break // no more input
		}
		if err != nil {
			return nil, fmt.Errorf("read error: %w", err)
		}
		if prefix != '*' {
			return nil, fmt.Errorf("invalid RESP: expected '*', got '%c'", prefix)
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read array header: %w", err)
		}
		numElements, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return nil, fmt.Errorf("invalid array length: %w", err)
		}

		parts := make([]string, 0, numElements)
		for i := 0; i < numElements; i++ {
			// Expect bulk string: $
			prefix, err := reader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("read error: %w", err)
			}
			if prefix != '$' {
				return nil, fmt.Errorf("invalid RESP: expected '$', got '%c'", prefix)
			}

			line, err := reader.ReadString('\n')
			if err != nil {
				return nil, fmt.Errorf("failed to read bulk length: %w", err)
			}
			strLen, err := strconv.Atoi(strings.TrimSpace(line))
			if err != nil {
				return nil, fmt.Errorf("invalid bulk length: %w", err)
			}

			data := make([]byte, strLen+2) // +2 for \r\n
			_, err = io.ReadFull(reader, data)
			if err != nil {
				return nil, fmt.Errorf("failed to read bulk string: %w", err)
			}
			parts = append(parts, string(data[:strLen]))
		}

		if len(parts) == 0 {
			return nil, fmt.Errorf("command must contain at least one element")
		}

		cmd := Command{
			Elements: numElements,
			Name:     strings.ToUpper(parts[0]),
			Args:     parts[1:],
		}
		commands = append(commands, cmd)
	}

	return commands, nil
}
func sliceWithStep(array []string, start int, step int) []string {
	result := []string{}
	for i := start; i < len(array); i += step {
		result = append(result, array[i])
	}
	return result
}
