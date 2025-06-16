package commands

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jorzel/myredis/app/config"
	"github.com/jorzel/myredis/app/protocol"

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

func TestHandleDel(t *testing.T) {
	delCommand := protocol.Command{
		Name: "DEL",
		Args: []string{"key", "key2"},
	}

	handler := NewCommandHandler(&config.Config{})
	// First, we need to set the value for the key
	setCommand := protocol.Command{
		Name: "SET",
		Args: []string{"key", "value"},
	}
	conn := &MockConn{}
	handler.Handle(context.Background(), conn, setCommand)

	_, err := handler.Handle(context.Background(), conn, delCommand)

	require.NoError(t, err, "Expected no error when handling DEL command")
	require.Len(t, conn.writes, 2, "Expected two writes to the connection")
	assert.Equal(t, string([]byte(":1\r\n")), string(conn.writes[1]), "Expected DEL command to return 1 (deleted)")
}

func TestHandleReplConfListeningPort(t *testing.T) {
	command := protocol.Command{
		Name: "REPLCONF",
		Args: []string{"listening-port", "6379"},
	}
	handler := NewCommandHandler(&config.Config{})

	conn := &MockConn{}
	_, err := handler.Handle(context.Background(), conn, command)

	require.NoError(t, err, "Expected no error when handling REPLCONF command")
	require.Len(t, conn.writes, 1, "Expected one write to the connection")
	assert.Equal(t, string([]byte("+OK\r\n")), string(conn.writes[0]), "Expected REPLCONF command to return OK")
}
