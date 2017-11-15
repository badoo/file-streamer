// Copyright (c) 2017 Badoo Tech. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file

// BUG(denkoren@corp.badoo.com): on kqueue systems (like MacOS X, for example) file_streamer can't stream named pipes.
// 'kqueue' can't notify about FIFO file events, so fsNotify just does not provide any signals on data writes to a FIFO
// file.

// Package file_streamer provides a file streaming service that streams all data from a file into a buffered writer.
//
// It uses fsNotify for detecting changes in files, reads that changes (new data) and writes them into a buffered writer.
package file_streamer

import (
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

const (
	stateStopped uint8 = 1 << iota
	stateStarting
	stateRunning
	stateStopping
)

type empty struct{}

// Maps watched file to the list of channels (readers) to be notified about changes detection.
type subscriptions map[string]map[newDataChan]empty

// Streamer is a main package instance that provides streaming service to all Listeners
type Streamer struct {
	mu sync.Mutex

	logger *log.Logger

	fsNotify         *fsnotify.Watcher
	changedFileNames chan string

	subscriptions subscriptions
	subscribe     chan *Listener
	unsubscribe   chan *Listener

	threads sync.WaitGroup

	state uint8
}

var (
	// ErrNotRunning is an error returned when action can't be finished because Streamer service is not running yet
	// Streamer.Start() was not called
	ErrNotRunning = errors.New("streamer is not running")

	// ErrRunning is an error returned when action can't be finished because Streamer service is in running state
	ErrRunning = errors.New("streamer is running")
)

// New creates new instance of file streamer. Usually, you don't need more than one instance of Streamer.
func New(logger *log.Logger) *Streamer {
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags)
	}

	s := &Streamer{
		logger: logger,

		subscriptions: make(subscriptions),
		subscribe:     make(chan *Listener),
		unsubscribe:   make(chan *Listener),

		state: stateStopped,
	}

	return s
}

// read all fs notifications and send changed file names to eventsRouter()
func (s *Streamer) sendChangeEvents() {
	defer close(s.changedFileNames)
	defer s.threads.Done()

	for {
		fileEvent, isOpen := <-s.fsNotify.Events
		if !isOpen {
			// We're not listening file changes any more (watcher is closed and no events left in pool)
			return
		}

		s.changedFileNames <- fileEvent.Name
	}
}

// We have to read notify errors in a separate goroutine to make sure all data is read from fsNotify.Errors,
// fsnotify.Watcher.Errors channel is unbuffered and that may cause memory leaks.
func (s *Streamer) logNotifyErrors() {
	defer s.threads.Done()

	for {
		notificationError, isOpen := <-s.fsNotify.Errors
		if !isOpen {
			// We're not listening file changes any more (watcher is closed and no errors left in pool)
			return
		}

		s.logger.Printf("FS notifications error: %s", notificationError)
	}
}

// eventsRouter will notify all subscribed channels about changes in particular file.
// subscribeListener adds listener's 'new data' notification channel to subscriptions list.
func (s *Streamer) subscribeListener(listener *Listener) {
	// if it's a first subscription for the given file - prepare subscriptions map and start to listen for file events
	if _, subscriptionExists := s.subscriptions[listener.file.Name()]; !subscriptionExists {
		s.subscriptions[listener.file.Name()] = make(map[newDataChan]empty)

		err := s.fsNotify.Add(listener.file.Name())
		if err != nil {
			s.logger.Printf("Failed to register new fsNotify listener for file '%s': %v", listener.file.Name(), err)
		}
	}

	// subscribe
	s.logger.Printf("New listener for '%s' file", listener.file.Name())
	s.subscriptions[listener.file.Name()][listener.newDataNotifications] = empty{}
}

// unsubscribeListener removes listener's 'new data' notification channel from subscriptions list.
func (s *Streamer) unsubscribeListener(listener *Listener) {
	// unsubscribe
	delete(s.subscriptions[listener.file.Name()], listener.newDataNotifications)
	s.logger.Printf("File '%s' listener unsubscribed", listener.file.Name())

	// when it was a last listener for the given file - stop listening and forget about file
	if len(s.subscriptions[listener.file.Name()]) == 0 {
		delete(s.subscriptions, listener.file.Name())

		err := s.fsNotify.Remove(listener.file.Name())
		if err != nil {
			s.logger.Printf("Failed stop listening fsNotify events of file '%s': %v", listener.file.Name(), err)
		}
	}
}

// eventsRouter receives filesystem events from fsNotify and sends 'new data' notifications to all subscribers.
func (s *Streamer) eventsRouter() {
	defer s.threads.Done()

routeEvents:
	for {
		select {
		case listener := <-s.subscribe:
			s.subscribeListener(listener)
		case listener := <-s.unsubscribe:
			s.unsubscribeListener(listener)
		case filename, isOpen := <-s.changedFileNames:
			if !isOpen {
				break routeEvents
			}

			if _, exists := s.subscriptions[filename]; !exists || len(s.subscriptions[filename]) == 0 {
				s.logger.Printf("No listeners subscribed for '%s' file events", filename)
				continue
			}

			for toNotify := range s.subscriptions[filename] {
				if len(toNotify) < cap(toNotify) {
					toNotify <- newDataEvent{}
				}
			}
		}
	}

	// Wait for all subscriptions to be finished
	for len(s.subscriptions) != 0 {
		select {
		case listener := <-s.subscribe:
			s.subscribeListener(listener)
		case listener := <-s.unsubscribe:
			s.unsubscribeListener(listener)
		}
	}
}

// SetLogger changes main Streamer logger
func (s *Streamer) SetLogger(l *log.Logger) {
	s.logger = l
}

// initialize Streamer instance before each .Start()
func (s *Streamer) init() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	s.fsNotify = watcher // we closed it during Stop() process

	s.changedFileNames = make(chan string, 1000) // we closed it during Stop() process

	return nil
}

// Start streamer.
//
// Makes Streamer's .StreamTo() available for Listeners. After return from this function Streamer is able to stream
// files into buffered writers.
//
// Thread safe.
func (s *Streamer) Start() error {
	s.mu.Lock()
	if s.state != stateStopped {
		s.mu.Unlock()
		return ErrRunning
	}
	s.state = stateStarting
	s.mu.Unlock()

	err := s.init()
	if err != nil {
		return err
	}

	// Attach to fs notification system
	s.threads.Add(2)
	go s.sendChangeEvents()
	go s.logNotifyErrors()

	// Start streaming service
	s.threads.Add(1)
	go s.eventsRouter()

	s.mu.Lock()
	s.state = stateRunning
	s.mu.Unlock()

	return nil
}

// Stop streamer.
//
// Prevents new streams creation, finishes all existing subscriptions and stops all internal goroutines.
//
// Thread safe.
func (s *Streamer) Stop() error {
	s.mu.Lock()
	if s.state != stateRunning {
		s.mu.Unlock()
		return ErrNotRunning
	}
	s.state = stateStopping
	s.mu.Unlock()

	s.fsNotify.Close() // trigger stop chain: fsNotify -> (sendChangeEvents,logNotifyErrors) -> eventsRouter
	s.threads.Wait()

	s.mu.Lock()
	s.state = stateStopped
	s.mu.Unlock()

	return nil
}

// IsRunning is a thread-safe way to check Streamer is in 'Running' state.
//
// 'Running' state means Streamer reads fsNotify events on files, reads data from files and streams it to
// buffered writers.
func (s *Streamer) IsRunning() bool {
	s.mu.Lock()
	isRunning := s.state == stateRunning
	s.mu.Unlock()

	return isRunning
}

// StreamTo makes streamer to start data streaming for Listener.
// StreamTo will block until <listener> is closed (listener.Close() is called) or file is not modified more than
// <timeout> time.
//
// Zero value ('0') as <timeout> disables timeout at all, making Streamer to stream data until Listener is closed
// (by .Close() call)
//
// returns ErrNotRunning when Streamer is not ready for streaming data (was not Start()'ed, or was Stop()'ed)
//
// returns ErrListenerClosed when listener is not ready for accepting data.
//
func (s *Streamer) StreamTo(listener *Listener, timeout time.Duration) error {
	if !s.IsRunning() {
		return ErrNotRunning
	}

	s.subscribe <- listener
	defer func() { s.unsubscribe <- listener }()

	listenerBufSize := listener.writeDataTo.Available() + listener.writeDataTo.Buffered()
	buf := make([]byte, listenerBufSize)

	timeoutTimer := getTimer(timeout)
	for {
		select {
		case _, isOpen := <-listener.newDataNotifications:
			if !isOpen {
				return nil
			}

			listener.file.Seek(0, 1) // re-set current position to be able to read to EOF again

			_, err := io.CopyBuffer(listener.writeDataTo, listener.file, buf)

			if err != nil {
				fmt.Fprintf(listener.writeDataTo, "Could not stream file data: %s", err.Error())
				_ = listener.writeDataTo.Flush()

				s.logger.Printf("File '%s' stream error: %s", listener.file.Name(), err.Error())
				return err
			}

			// Force all data to be sent to client
			err = listener.writeDataTo.Flush()
			if err != nil {
				s.logger.Printf("File '%s' stream error: %s", listener.file.Name(), err.Error())
				return err
			}

			// Is file exist? If not - just stop streaming
			if _, err = os.Stat(listener.file.Name()); err != nil {
				return nil
			}
		case <-timeoutTimer.C:
			// Just stop streaming after <timeout> of inactivity (no changes in file)
			return nil
		}

		if timeout != 0 {
			timeoutTimer.Reset(timeout)
		}
	}
}
