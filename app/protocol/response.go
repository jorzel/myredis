package protocol

import (
	"bufio"
	"fmt"
	"strings"
)

const (
	SimpleStringType = "+"
	ErrorType        = "-"
	IntegerType      = ":"
	BulkStringType   = "$"
)

type Response struct {
	Type  string // "+", "-", ":", etc.
	Value string
}

func ParseResponse(reader *bufio.Reader) (*Response, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read response prefix: %w", err)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read response line: %w", err)
	}
	line = strings.TrimSuffix(line, "\r\n")

	return &Response{
		Type:  string(prefix),
		Value: line,
	}, nil
}
