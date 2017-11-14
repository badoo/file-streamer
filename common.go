package file_streamer

import "time"

// Makes 'inifinite' timer when duration is zero.
func getTimer(duration time.Duration) *time.Timer {
	if duration == 0 {
		// A hacky way to make timer inifinite. It sends data into .C channel only once when it ticked.
		t := time.NewTimer(-time.Millisecond)
		<-t.C
		return t
	}

	return time.NewTimer(duration)
}
