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
					Elements: 2,
					Name:     "ECHO",
					Args:     []string{"key"},
				},
			},
		},
		{
			name:       "Valid SET command",
			rawMessage: []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
			expectedCommands: []Command{
				{
					Elements: 3,
					Name:     "SET",
					Args:     []string{"key", "value"},
				},
			},
		},
		{
			name:       "Valid GET command",
			rawMessage: []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
			expectedCommands: []Command{
				{
					Elements: 2,
					Name:     "GET",
					Args:     []string{"key"},
				},
			},
		},
		{
			name:       "Two commands in one message",
			rawMessage: []byte("*1\r\n$4\r\nPING\r\n*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6780\r\n"),
			expectedCommands: []Command{
				{
					Elements: 1,
					Name:     "PING",
					Args:     []string{},
				},
				{
					Elements: 3,
					Name:     "REPLCONF",
					Args:     []string{"listening-port", "6780"},
				},
			},
		},
		{
			name:       "Two replconf commands in one message",
			rawMessage: []byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6780\r\n*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"),
			expectedCommands: []Command{
				{
					Elements: 3,
					Name:     "REPLCONF",
					Args:     []string{"listening-port", "6780"},
				},
				{
					Elements: 3,
					Name:     "REPLCONF",
					Args:     []string{"capa", "psync2"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewCommandParser()
			commands, err := parser.Parse(tt.rawMessage)
			require.NoError(t, err, "Expected no error parsing command")
			require.Equal(t, len(tt.expectedCommands), len(commands), "Expected number of commands to match")
			for i := range commands {
				assert.Equal(t, tt.expectedCommands[i].Elements, commands[i].Elements, "Elements count mismatch")
				assert.Equal(t, tt.expectedCommands[i].Name, commands[i].Name, "Command name mismatch")
				assert.Equal(t, tt.expectedCommands[i].Args, commands[i].Args, "Command arguments mismatch")
			}
		})
	}
}
