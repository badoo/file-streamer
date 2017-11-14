# FileStreamer
Streams given file data into any buffered writer. Uses fsNotify for new data detection in files.

# Concepts

The library consists of 2 main instances:

Listener:
```
listener, err := file_streamer.NewListener(<file to watch>, <buffer to write data to>)
```

Streamer:
```
streamer := file_streamer.New(<logger>)
err := streamer.Start()
```

Streamer is a heart of package. In most cases you don't need to create more than one Streamer in your application.

Listener represents a Streamer 'subscription' for data streaming,
it binds file to be streamed and buffered writer to be used as a file data receiver.

### Examples

The minimal working example (and most trivial I can imagine) is:

```
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
```

It provides similar functionality to GNU `tail -f` command, but with streaming 
start from the beginning of the file.

You can find more examples in 'examples/' directory of the package.
