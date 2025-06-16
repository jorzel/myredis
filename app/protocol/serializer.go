package protocol

import (
	"fmt"
	"strconv"
)

// SimpleInteger serializes an integer into the Redis protocol simple integer format.
// Example: 42 becomes ":42\r\n"
func SimpleInteger(i int) []byte {
	return []byte(":" + strconv.Itoa(i) + CRLF)
}



// SimpleString serializes a simple string into the Redis protocol simple string format.
// Example: "hello" becomes "+hello\r\n"
func SimpleString(s string) []byte {
	return []byte("+" + s + CRLF)
}

// Error serializes an error message into the Redis protocol error format.
// Example: "ERR some error" becomes "-ERR some error\r\n"
func Error(s string) []byte {
	return []byte("-ERR " + s + CRLF)
}

// BulkString serializes a string into the Redis protocol bulk string format.
// Example: "hello" becomes "$5\r\nhello\r\n"
func BulkString(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + CRLF + s + CRLF)
}

// Nil serializes a nil value into the Redis protocol nil bulk string format.
// Example: nil becomes "$-1\r\n"
func Nil() []byte {
	return []byte("$-1" + CRLF)
}

// BulkArray serializes an array of strings into the Redis protocol bulk array format.
// Example: ["ECHO", "key"] becomes "*2\r\n$4\r\nECHO\r\n$3\r\nkey\r\n"
func BulkArray(elements []string) []byte {
	// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
	result := "*" + strconv.Itoa(len(elements)) + CRLF
	for _, element := range elements {
		result += string(BulkString(element))
	}
	return []byte(result)
}

// FileContent serializes file content into the Redis protocol bulk string format.
// It uses a bulk string with the length of the content without CRLF at the end.
func FileContent(content []byte) []byte {
	// For file content, we use a bulk string with the length of the content
	return []byte(fmt.Sprintf("$%d%s%s", len(content), CRLF, content))
}
