// Here is an example of raw file streaming service that uses HTTP requests for picking a file to stream.
// It does not provide a valid HTTP responses to clients, but streams requested files into a connections instead.
// Example:
//     go run ./examples/stream-to-http.go
//
//   while stream-to-http.go process is running, requests like
//     curl "http://localhost:4444/log-stream/LICENCE"
//
//   will start streaming of LICENCE file from current directory with 2s timeout on changes.

package main

import (
	"fmt"
	"github.com/badoo/file-streamer"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"
)

type rawStreamHandler struct {
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

func (h *rawStreamHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	filePath := path.Join(h.pathPrefix, req.URL.Path)

	req.ParseForm()

	offset, err := parseOffset(req.Form.Get("offset"))
	if err != nil {
		http.Error(w, fmt.Sprintf("incorrect offset: %s", err.Error()), http.StatusBadRequest)
		return
	}

	err = file_streamer.StreamRawData(filePath, offset, h.streamer, w, time.Second*2)
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

	handler := &rawStreamHandler{
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
