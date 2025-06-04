package commands

import (
	"fmt"
	"net"
)

type ConnectionPool interface {
	Add(conn net.Conn) error
	GetAll() []net.Conn
}

var _ ConnectionPool = (*DefaultConnectionPool)(nil)

func NewConnectionPool() ConnectionPool {
	return &DefaultConnectionPool{
		conns: make(map[string]net.Conn), // Initialize the map to store connections
	}
}

type DefaultConnectionPool struct {
	conns map[string]net.Conn // For storing connections to clients
}

func (h *DefaultConnectionPool) Add(conn net.Conn) error {
	connID := h.getConnID(conn)
	if connID == "" {
		return fmt.Errorf("failed to get connection ID, connection is nil")
	}
	h.conns[connID] = conn
	return nil
}

func (h *DefaultConnectionPool) GetAll() []net.Conn {
	var allConns []net.Conn
	for _, conn := range h.conns {
		allConns = append(allConns, conn)
	}
	return allConns
}

func (h *DefaultConnectionPool) getConnID(conn net.Conn) string {
	if conn == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}
