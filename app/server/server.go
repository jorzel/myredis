package server

import (
	"context"
)

const (
	MasterRole = "master"
)

type Server interface {
	Start(ctx context.Context) error
}
