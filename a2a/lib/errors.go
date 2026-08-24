package lib

import "fmt"

// ProtocolError is a violation of the a2a-jetstream layering or lifecycle
// rules. It is surfaced, never passed through.
type ProtocolError struct {
	Msg string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("a2a protocol error: %s", e.Msg)
}

// A2AError is the JSON-RPC-shaped error A2A defines; the library fails
// client-side with one before publishing anything the bus would reject.
type A2AError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *A2AError) Error() string {
	return fmt.Sprintf("a2a error %d: %s", e.Code, e.Message)
}

// A2A / JSON-RPC error codes used by the library.
const (
	CodeInvalidParams   = -32602
	CodeContentTooLarge = -32011
)
