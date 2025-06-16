package config

const (
	MasterRole  = "master"
	ReplicaRole = "replica"
)

type Node struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type Config struct {
	ReplicaOf  *Node `json:"replica_of"`
	ServerPort int   `json:"port"`
}
