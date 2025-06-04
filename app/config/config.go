package config

import (
	"crypto/rand"
	"math/big"
)

type InitSpec struct {
	ReplicaOf  *Node `json:"replica_of,omitempty"`
	ServerPort int   `json:"port"`
}

type Node struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type Config struct {
	ReplicaOf     *Node  `json:"replica_of"`
	ReplicationID string `json:"replication_id"`
	ServerPort    int    `json:"port"`
}

func NewConfig(initSpec InitSpec) (*Config, error) {
	replID, err := generateReplicaID()
	if err != nil {
		return nil, err
	}
	return &Config{
		ReplicaOf:     initSpec.ReplicaOf,
		ReplicationID: replID,
		ServerPort:    initSpec.ServerPort,
	}, nil
}

func (c *Config) IsSlave() bool {
	return c.ReplicaOf != nil
}

const replIDLength = 40
const alphanum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateReplicaID() (string, error) {
	result := make([]byte, replIDLength)
	for i := range result {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphanum))))
		if err != nil {
			return "", err
		}
		result[i] = alphanum[n.Int64()]
	}
	return string(result), nil
}
