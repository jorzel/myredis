package commands

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ net.Conn = (*MockConn)(nil)

type MockConn struct {
	writes [][]byte
}

func (m *MockConn) Write(b []byte) (int, error) {
	m.writes = append(m.writes, b)
	return len(b), nil
}
func (m *MockConn) Read(b []byte) (int, error) {
	return 0, nil
}
func (m *MockConn) Close() error {
	return nil
}
func (m *MockConn) LocalAddr() net.Addr {
	return nil
}
func (m *MockConn) RemoteAddr() net.Addr {
	return nil
}
func (m *MockConn) SetDeadline(t time.Time) error {
	return nil
}
func (m *MockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestHandlePing(t *testing.T) {
	command := protocol.Command{
		Name: "PING",
	}
	conn := &MockConn{}
	handler := NewCommandHandler(&config.Config{})
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling PING command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("+PONG\r\n")), string(conn.writes[0]), "Expected PING command to return PONG")
}

func TestHandleEcho(t *testing.T) {
	command := protocol.Command{
		Name: "ECHO",
		Args: []string{"as"},
	}

	conn := &MockConn{}
	handler := NewCommandHandler(&config.Config{})
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling ECHO command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("$2\r\nas\r\n")), string(conn.writes[0]), "Expected ECHO command to return the same string")
}

func TestHandleSet(t *testing.T) {
	command := protocol.Command{
		Name: "SET",
		Args: []string{"key", "value"},
	}

	conn := &MockConn{}
	handler := NewCommandHandler(&config.Config{})
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling SET command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("+OK\r\n")), string(conn.writes[0]), "Expected SET command to return OK")
}

func TestHandleSetWithPX(t *testing.T) {
	command := protocol.Command{
		Name: "SET",
		Args: []string{"key", "value", "px", "1000"},
	}

	conn := &MockConn{}
	handler := NewCommandHandler(&config.Config{})
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling SET command with PX")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("+OK\r\n")), string(conn.writes[0]), "Expected SET command to return OK")
}

func TestHandleGet(t *testing.T) {
	getCommand := protocol.Command{
		Name: "GET",
		Args: []string{"key"},
	}

	handler := NewCommandHandler(&config.Config{})
	// First, we need to set the value for the key
	setCommand := protocol.Command{
		Name: "SET",
		Args: []string{"key", "value"},
	}
	conn := &MockConn{}
	handler.Handle(context.Background(), conn, setCommand)

	_, err := handler.Handle(context.Background(), conn, getCommand)

	require.NoError(t, err, "Expected no error when handling GET command")
	require.Len(t, conn.writes, 2, "Expected one write to the connection")
	assert.Equal(
		t,
		string([]byte("$5\r\nvalue\r\n")),
		string(conn.writes[1]),
		"Expected GET command to return the value for the key",
	)
}

func TestHandleGetExpiredRecord(t *testing.T) {
	getCommand := protocol.Command{
		Name: "GET",
		Args: []string{"key"},
	}

	handler := NewCommandHandler(&config.Config{})
	// First, we need to set the value for the key
	setCommand := protocol.Command{
		Name: "SET",
		Args: []string{"key", "value", "px", "1"},
	}
	conn := &MockConn{}
	handler.Handle(context.Background(), conn, setCommand)
	time.Sleep(2 * time.Millisecond) // Wait for the key to expire

	_, err := handler.Handle(context.Background(), conn, getCommand)

	require.NoError(t, err, "Expected no error when handling GET command for expired key")
	require.Len(t, conn.writes, 2, "Expected two writes to the connection")
	assert.Equal(t, string([]byte("$-1\r\n")), string(conn.writes[1]), "Expected GET command to return nil")
}

func TestHandleInfo(t *testing.T) {
	command := protocol.Command{
		Name: "INFO",
	}

	config := &config.Config{}
	config.ReplicationID = "12" // Mock ReplID for testing
	handler := NewCommandHandler(config)
	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling INFO command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(
		t,
		string([]byte("$65\r\n#Replication\r\nmaster_replid:12\r\nrole:master\r\nmaster_repl_offset:0\r\n")),
		string(conn.writes[0]),
		"Expected INFO command to return the same string",
	)
}

func TestHandleInfoSlave(t *testing.T) {
	command := protocol.Command{
		Name: "INFO",
	}

	config := &config.Config{
		ReplicaOf: &config.Node{
			Host: "host",
			Port: 6378,
		},
	}
	handler := NewCommandHandler(config)
	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling INFO command for slave")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(
		t,
		string([]byte("$24\r\n#Replication\r\nrole:slave\r\n")),
		string(conn.writes[0]),
		"Expected INFO command to return the same string",
	)
}

func TestHandleReplConfListeningPort(t *testing.T) {
	command := protocol.Command{
		Name: "REPLCONF",
		Args: []string{"listening-port", "6379"},
	}

	config := &config.Config{}
	handler := NewCommandHandler(config)
	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling REPLCONF command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("+OK\r\n")), string(conn.writes[0]), "Expected REPLCONF command to return OK")
}

func TestHandleReplConfCapa(t *testing.T) {
	command := protocol.Command{
		Name: "REPLCONF",
		Args: []string{"capa", "psync2"},
	}

	config := &config.Config{}
	handler := NewCommandHandler(config)
	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling REPLCONF command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("+OK\r\n")), string(conn.writes[0]), "Expected REPLCONF command to return OK")
}

func TestHandleReplConfInvalid(t *testing.T) {
	command := protocol.Command{
		Name: "REPLCONF",
		Args: []string{"invalid", "argument"},
	}

	config := &config.Config{}
	handler := NewCommandHandler(config)
	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected REPLCONF command to return an error response")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t,
		string([]byte("-ERR REPLCONF command only supports 'listening-port <PORT>' and 'capa psync2' arguments\r\n")),
		string(conn.writes[0]),
		"Expected REPLCONF command to return an error",
	)
}

func TestHandlePSync(t *testing.T) {
	command := protocol.Command{
		Name: "PSYNC",
		Args: []string{"?", "-1"},
	}

	config := &config.Config{}
	config.ReplicationID = "12" // Mock ReplID for testing
	handler := NewCommandHandler(config)
	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling PSYNC command")
	require.Len(t, conn.writes, 2, "Expected two writes to the connection")
	assert.Equal(t,
		string([]byte("+FULLRESYNC 12 0\r\n")),
		string(conn.writes[0]),
		"Expected PSYNC command to return FULLRESYNC response for master",
	)
	assert.NotNil(t, conn.writes[1], "Expected second write to contain DB file content")
}
