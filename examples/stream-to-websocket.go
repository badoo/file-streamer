// Here is a dirty implementation of WebSocket streamer.
//
// It should be considered as a Proof of Concept.
//
// It may fail in some conditions (not fully tested), and probably has some lacks of implementations required by
// gorilla.WebSocket package, but it demonstrates the idea of using Streamer with WebSocket connections.

package main

import (
	"bufio"
	"fmt"
	"github.com/badoo/file-streamer"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"
)

// webSocketWriter is a wrapper for gorilla WebSocket connection with io.Writer interface implementation.
//
// it allows you use WebSocket connection as a regular io.Writer to send to client text or binary messages.
type webSocketWriter struct {
	io.Writer

	msgType int
	conn    *websocket.Conn
}

// Write just implements io.Writer interface.
//
// Any data provided to Write() will be sent to client through provided WebSocket connection with message type provided
// at writer initialization.
func (ws *webSocketWriter) Write(p []byte) (n int, err error) {
	err = ws.conn.WriteMessage(ws.msgType, p)
	return len(p), err
}

// newBuffWSWriter is a simple wrapper for creating a buffered writer for WebSocket connection.
//
// All data written to this writer will be sent to client in message(s) of given type.
func newBuffWSWriter(conn *websocket.Conn, msgType int) *bufio.Writer {
	if msgType == 0 {
		msgType = websocket.BinaryMessage
	}

	return bufio.NewWriter(
		&webSocketWriter{
			conn:    conn,
			msgType: msgType,
		},
	)
}

type wsStreamHandler struct {
	http.Handler

	pathPrefix string
	streamer   *file_streamer.Streamer
}

func parseOffset(offsetString string) (int64, error) {
	if offsetString == "" {
		return 0, nil
	}

	return strconv.ParseInt(offsetString, 10, 64)
}

func openWithOffset(filePath string, offset int64, flags int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(filePath, flags, perm)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(offset, 0)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func switchToWebSocket(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*websocket.Conn, error) {
	var connUpgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	return connUpgrader.Upgrade(w, r, responseHeader)
}

func readWebSocketInput(conn *websocket.Conn) error {
	for {
		mType, data, err := conn.ReadMessage()

		if err != nil {
			// Check it was regular close
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived) {
				log.Println("Connection closed")
				return nil
			}

			return err
		}

		switch mType {
		case websocket.CloseMessage:
			log.Println("Connection closed by client")
			return nil

		case websocket.BinaryMessage:
			log.Printf("Received binary data from client: %v", data)

		case websocket.TextMessage:
			log.Printf("Received text data from client: %s", data)

		case websocket.PongMessage:
			log.Printf("Received pong message from client: %v", data)

		case websocket.PingMessage:
			// According to documentation WriteControl are thread-safe so we can ignore we already have a stream
			// (file streamer) that writes data into WebSocket connection
			conn.WriteControl(
				websocket.PongMessage,
				data,
				time.Now().Add(time.Second*2),
			)
		}
	}
}

func (h *wsStreamHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	conn, err := switchToWebSocket(w, req, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		conn.WriteControl(websocket.CloseMessage, []byte{}, time.Now().Add(time.Second))
	}()

	filePath := path.Join(h.pathPrefix, req.URL.Path)

	req.ParseForm()

	offset, err := parseOffset(req.Form.Get("offset"))
	if err != nil {
		conn.WriteControl(
			websocket.CloseMessage,
			[]byte(fmt.Sprintf("incorrect offset: %s", err.Error())),
			time.Now().Add(time.Second),
		)
		return
	}

	file, err := openWithOffset(filePath, offset, os.O_RDONLY, 0)
	if err != nil {
		conn.WriteControl(
			websocket.CloseMessage,
			[]byte(err.Error()),
			time.Now().Add(time.Second),
		)
		return
	}

	go readWebSocketInput(conn)

	listener := file_streamer.NewListener(file, newBuffWSWriter(conn, websocket.TextMessage))
	// Stream file data to WebSocket client
	// Since gorilla WebSockets implementation does not support concurrent writes,
	// keep in mind you shouldn't write to WebSocket connection while Streamer is attached to it.
	err = h.streamer.StreamTo(listener, time.Second*2)
	if err != nil {
		log.Println("file streaming error:", err.Error())
	}
}

func main() {
	streamer := file_streamer.New(log.New(os.Stderr, "[streamer] ", log.LstdFlags))

	err := streamer.Start()
	if err != nil {
		log.Fatalln(err)
	}

	handler := &wsStreamHandler{
		pathPrefix: "./", // stream any file in current directory and its subs
		streamer:   streamer,
	}

	mux := http.NewServeMux()
	mux.Handle("/log-stream/", http.StripPrefix("/log-stream/", handler))

	err = http.ListenAndServe(":4444", mux)
	if err != http.ErrServerClosed {
		log.Fatalln(err)
	}
}
