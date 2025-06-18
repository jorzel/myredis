package protocol

// CRLF is the standard line ending for Redis protocol messages.
const CRLF = "\r\n"

const (
	PING       = "PING"
	ECHO       = "ECHO"
	SET        = "SET"
	GET        = "GET"
	DEL        = "DEL"
	REPLCONF   = "REPLCONF"
	PSYNC      = "PSYNC"
	FULLRESYNC = "FULLRESYNC"
)
