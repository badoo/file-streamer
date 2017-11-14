// This trivial example has similar functionality to GNU 'tail -f' command, except it won't
// start streaming from the last 10 lines of target file, but will stream all its contents.

package main

import (
	"bufio"
	"github.com/badoo/file-streamer"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	nullLogger := log.New(ioutil.Discard, "", 0)

	streamer := file_streamer.New(nullLogger)
	err := streamer.Start()
	if err != nil {
		log.Fatalln(err)
	}

	targetFile := os.Args[1]
	readFrom, err := os.Open(targetFile)
	if err != nil {
		log.Fatalln(err)
	}

	logTo := bufio.NewWriter(os.Stdout)

	listener := file_streamer.NewListener(readFrom, logTo)
	streamer.StreamTo(listener, 0)
}
