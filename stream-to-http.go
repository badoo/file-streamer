package file_streamer

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

func hijack(w http.ResponseWriter) (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("WebServer doesn't support hijacking")
	}

	conn, connBuffer, err := hj.Hijack()
	if err != nil {
		return conn, connBuffer, fmt.Errorf("failed to hijack connection: %s", err.Error())
	}

	return conn, connBuffer, nil
}

// StreamRawData hijacks HTTP connection and sends raw file data into a connection buffer.
//
// It does not send any additional information like HTTP headers and does not check connection in any way before
// sending data.
// Being used 'as is' it breaks HTTP protocol (sends no headers to a clien) and should not be considered as a correct
// way to send file data as a part of regular HTTP interaction.
//
// To stream file data inside a valid HTTP response without breaking a connection structure, send headers and any other
// metadata you want to a client before calling StreamRawData().
func StreamRawData(filePath string, initialOffset int64, streamer *Streamer, w http.ResponseWriter, timeout time.Duration) error {
	if !streamer.IsRunning() {
		http.Error(w, "Streaming service is not running", http.StatusServiceUnavailable)
		return ErrNotRunning
	}

	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, "Can't open file for streaming: "+err.Error(), http.StatusNotFound)
		return err
	}
	defer file.Close()

	_, err = file.Seek(initialOffset, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}

	conn, connBuffer, err := hijack(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	defer conn.Close()

	listener := NewListener(file, connBuffer.Writer)
	err = streamer.StreamTo(listener, timeout)

	switch err {
	case nil:
		// no error -> do nothing

	case ErrNotRunning:
		fmt.Fprintln(connBuffer, "--------------------------------")
		fmt.Fprintln(connBuffer, "Streaming service is not running")
		_ = connBuffer.Flush()

	default:
		fmt.Fprintln(connBuffer, "--------------------------------------------------------")
		fmt.Fprintf(connBuffer, "file streaming error: %v", err)
		_ = connBuffer.Flush()
	}

	return err
}
