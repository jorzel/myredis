package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	// test cases for command parsing
	tests := []struct {
		name             string
		rawMessage       []byte
		expectedCommands []Command
	}{
		{
			name:       "Valid ECHO command",
			rawMessage: []byte("*2\r\n$4\r\nECHO\r\n$3\r\nkey\r\n"),
			expectedCommands: []Command{
				{
					Name: "ECHO",
					Args: []string{"key"},
				},
			},
		},
		{
			name:       "Valid SET command",
			rawMessage: []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
			expectedCommands: []Command{
				{
					Name: "SET",
					Args: []string{"key", "value"},
				},
			},
		},
		{
			name:       "Valid GET command",
			rawMessage: []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
			expectedCommands: []Command{
				{
					Name: "GET",
					Args: []string{"key"},
				},
			},
		},
		{
			name:       "Valid DEL command",
			rawMessage: []byte("*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n"),
			expectedCommands: []Command{
				{
					Name: "DEL",
					Args: []string{"key"},
				},
			},
		},
		{
			name:       "Two commands in one message",
			rawMessage: []byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"),
			expectedCommands: []Command{
				{
					Name: "PING",
					Args: []string{},
				},
				{
					Name: "PING",
					Args: []string{},
				},
			},
		},
		{
			name:       "Two replconf commands in one message",
			rawMessage: []byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6780\r\n*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"),
			expectedCommands: []Command{
				{
					Name: "REPLCONF",
					Args: []string{"listening-port", "6780"},
				},
				{
					Name: "REPLCONF",
					Args: []string{"capa", "psync2"},
				},
			},
		},
		{
			name:             "DB file",
			rawMessage:       []byte("$9\r\nREDIS0011"),
			expectedCommands: []Command{},
		},
		{
			name:       "Db file + REPLCONF GETACK",
			rawMessage: []byte("$9\r\nREDIS0011*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"),
			expectedCommands: []Command{
				{
					Name: "REPLCONF",
					Args: []string{"GETACK", "*"},
				},
			},
		},
		{
			name:       "FULLRESYNC command",
			rawMessage: []byte("+FULLRESYNC repl-id 0\r\n"),
			expectedCommands: []Command{
				{
					Name: "FULLRESYNC",
					Args: []string{"repl-id", "0"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewCommandParser()
			result, err := parser.Parse(tt.rawMessage)
			require.NoError(t, err, "Expected no error parsing command")
			require.Equal(t, len(tt.expectedCommands), len(result.Commands), "Expected number of commands to match")
			for i := range result.Commands {
				assert.Equal(t, tt.expectedCommands[i].Name, result.Commands[i].Name, "Command name mismatch")
				assert.Equal(t, tt.expectedCommands[i].Args, result.Commands[i].Args, "Command arguments mismatch")
			}
		})
	}
}
