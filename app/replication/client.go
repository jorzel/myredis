package replication

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/rs/zerolog"
)

type ClientFactory interface {
	GetClient(address string) (*Client, error)
	GetClientUsingConn(conn net.Conn) *Client
}

var _ ClientFactory = DefaultClientFactory{}

type DefaultClientFactory struct{}

func NewClientFactory() ClientFactory {
	return DefaultClientFactory{}
}

func (f DefaultClientFactory) GetClient(address string) (*Client, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server at %s: %w", address, err)
	}
	return NewClient(conn), nil
}

func (f DefaultClientFactory) GetClientUsingConn(conn net.Conn) *Client {
	return NewClient(conn)
}

type Client struct {
	Connection net.Conn
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		Connection: conn,
	}
}

func (s *Client) Propagate(ctx context.Context, command protocol.Command) error {
	if command.Name == protocol.SET {
		return s.PropagateSet(ctx, command)
	}

	return fmt.Errorf("unsupported command for propagation: %s", command.Name)

}

func (s *Client) PropagateSet(ctx context.Context, command protocol.Command) error {
	logger := zerolog.Ctx(ctx).With().Timestamp().Logger()
	content := []string{protocol.SET}
	for _, value := range command.Args {
		content = append(content, value)
	}
	msg := protocol.BulkArray(content)
	logger.Debug().Str("message", string(msg)).Msg("Sending SET command for propagation")
	_, err := s.Connection.Write(msg)
	if err != nil {
		return fmt.Errorf("failed to send SET command: %w", err)
	}
	return nil
}

func (s *Client) Ping(ctx context.Context) error {
	logger := zerolog.Ctx(ctx).With().Timestamp().Logger()
	msg := protocol.BulkArray([]string{protocol.PING})
	logger.Debug().Str("message", string(msg)).Msg("Sending REPLCONF command")
	_, err := s.Connection.Write(msg)
	if err != nil {
		return fmt.Errorf("failed to send PING command: %w", err)
	}
	resp, err := readLine(s.Connection)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("expected +PONG, got %q", resp)
	}
	return nil
}

func (s *Client) ReplConf(ctx context.Context, args []string) error {
	logger := zerolog.Ctx(ctx).With().Timestamp().Logger()
	content := []string{protocol.REPLCONF}
	if len(args) > 0 {
		content = append(content, args...)
	}
	msg := protocol.BulkArray(content)
	logger.Debug().Str("message", string(msg)).Msg("Sending REPLCONF command")
	_, err := s.Connection.Write(protocol.BulkArray(content))
	if err != nil {
		return fmt.Errorf("failed to send REPLCONF command: %w", err)
	}
	resp, err := readLine(s.Connection)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected +OK, got %q", resp)
	}
	return nil
}

func (s *Client) PSync(ctx context.Context, args []string) error {
	logger := zerolog.Ctx(ctx).With().Timestamp().Logger()
	content := []string{protocol.PSYNC}
	if len(args) > 0 {
		content = append(content, args...)
	}
	msg := protocol.BulkArray(content)
	logger.Debug().Str("message", string(msg)).Msg("Sending PSYNC command")
	_, err := s.Connection.Write(msg)
	if err != nil {
		return fmt.Errorf("failed to send PSYNC command: %w", err)
	}
	resp, err := readLine(s.Connection)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+FULLRESYNC") {
		return fmt.Errorf("expected +OK, got %q", resp)
	}

	return nil
}

func readLine(conn net.Conn) (string, error) {
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 1)
	for {
		_, err := conn.Read(tmp)
		if err != nil {
			return "", err
		}
		if tmp[0] == '\n' && len(buf) > 0 && buf[len(buf)-1] == '\r' {
			return string(buf[:len(buf)-1]), nil // strip \r
		}
		buf = append(buf, tmp[0])
	}
}
