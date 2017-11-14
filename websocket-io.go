package file_streamer

import (
	"bufio"
	"github.com/gorilla/websocket"
	"io"
)

// webSocketWriter is a wrapper for gorilla WebSocket connection with io.Writer interface implementation.
//
// it allows you use WebSocket connection as a regular io.Writer to send to client text or binary messages.
type webSocketWriter struct {
	io.Writer

	msgType int
	conn    *websocket.Conn
}

// NewWSWriter creates wrapper for WebSocket connection, exposing regular io.Writer interface.
//
// All data written to this writer will be sent to client over WebSocket connection in message(s) of specific type.
//
// Use gorilla/websocket message types here (e.g. websocket.BinaryMessage or websocket.TextMessage)
//
// Initially this wrapper was created for sending data (like Binary or Text) messages to the client,
// but it will not prevent you from sending control messages (like Close or Ping) as well.
func NewWSWriter(conn *websocket.Conn, msgType int) io.Writer {
	if msgType == 0 {
		msgType = websocket.BinaryMessage
	}

	return &webSocketWriter{
		msgType: msgType,
		conn:    conn,
	}
}

// Write just implements io.Writer interface.
//
// Any data provided to Write() will be sent to client through provided WebSocket connection with message type provided
// at writer initialization.
func (ws *webSocketWriter) Write(p []byte) (n int, err error) {
	err = ws.conn.WriteMessage(ws.msgType, p)
	return len(p), err
}

// NewBuffWSWriter is a simple wrapper for creating a buffered writer for WebSocket connection.
//
// All data written to this writer will be sent to client in message(s) of given type.
func NewBuffWSWriter(conn *websocket.Conn, msgType int) *bufio.Writer {
	return bufio.NewWriter(NewWSWriter(conn, msgType))
}
