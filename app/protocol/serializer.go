package protocol

import (
	"fmt"
	"strconv"
)

const CRLF = "\r\n"

func SimpleString(s string) []byte {
	return []byte("+" + s + CRLF)
}

func Error(s string) []byte {
	return []byte("-ERR " + s + CRLF)
}

func BulkString(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + CRLF + s + CRLF)
}

func Nil() []byte {
	return []byte("$-1" + CRLF)
}

func BulkArray(elements []string) []byte {
	// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
	result := "*" + strconv.Itoa(len(elements)) + CRLF
	for _, element := range elements {
		result += string(BulkString(element))
	}
	return []byte(result)
}

func FileContent(content []byte) []byte {
	// For file content, we use a bulk string with the length of the content
	return []byte(fmt.Sprintf("$%d\r\n%s", len(content), content))
}
