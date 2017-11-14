package file_streamer

import (
	"bufio"
	"os"
	"sync"
)

type (
	newDataEvent struct{}
	newDataChan  chan newDataEvent
)

// Listener is used for making Streamer to stream data from specific sile to specific buffered writer.
// Simply, it binds some *os.File to some *bufio.Writer
// Use NewListener for getting initialized Listener structure ready for usage in Streamer.
type Listener struct {
	mu sync.Mutex

	file        *os.File      // read data from file
	writeDataTo *bufio.Writer // file data will be written to this buffer

	newDataNotifications newDataChan
	isClosed             bool
}

// NewListener creates initialized Listener ready to be provided to Streamer.StreamTo() function
func NewListener(file *os.File, writeDataTo *bufio.Writer) *Listener {
	return &Listener{
		file:        file,
		writeDataTo: writeDataTo,

		newDataNotifications: make(newDataChan, 100),
		isClosed:             false,
	}
}

// Close prevents Streamer to stream any more data to this listener.
//
// Listeners are not reusable. After streaming was stopped (listener was closed), you can't use listener again for another streaming.
func (bs *Listener) Close() {
	bs.mu.Lock()

	if bs.isClosed {
		bs.mu.Unlock()
		return
	}

	close(bs.newDataNotifications)
	bs.isClosed = true

	bs.mu.Unlock()
}

// IsClosed returns true when listener is not available to receive data from the file and send it to the buffer any more.
func (bs *Listener) IsClosed() bool {
	bs.mu.Lock()
	closed := bs.isClosed
	bs.mu.Unlock()

	return closed
}
