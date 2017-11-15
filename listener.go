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
	l := &Listener{
		file:        file,
		writeDataTo: writeDataTo,

		newDataNotifications: make(newDataChan, 100),
		isClosed:             false,
	}

	// Force initial read.
	// This hack makes sure streamer will send contents of file to buffer even when nobody changes the watched file.
	l.newDataNotifications <- newDataEvent{}
	return l
}

// Close prevents Streamer to stream any more data to this listener.
//
// Listeners are not reusable. Reuse of closed listener will cause streamer to stop streaming immediately.
// There is one exception for it. The code:
//
//   l := NewListener(...)
//   l.Close()
//   streamer.StreamTo(l, <any timeout>)
//
// will cause streamer to read data from file exactly once. It implements 'cat' utility -like behaviour
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
