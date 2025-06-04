package protocol

import "slices"

const (
	PING     = "PING"
	ECHO     = "ECHO"
	SET      = "SET"
	GET      = "GET"
	INFO     = "INFO"
	REPLCONF = "REPLCONF"
	PSYNC    = "PSYNC"
)

var WriteCommnads = []string{SET}

func IsWriteCommand(command Command) bool {
	return slices.Contains(WriteCommnads, command.Name)
}
