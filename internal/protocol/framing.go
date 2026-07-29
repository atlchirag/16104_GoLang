package protocol

import (
	"bufio"
	"bytes"
)

// This file fixes finding D5 from the architecture plan. The .NET
// CustomReceiveFilter assumes one socket read == one packet, which is wrong:
// TCP is a byte stream with no message boundaries. A single read may deliver
// half a packet, or two packets stuck together.
//
// The correct approach is a bufio.Scanner with a custom split function. The
// scanner keeps unconsumed bytes between reads, so a partial packet is buffered
// until the rest arrives, and two packets in one read come back as two frames.
//
// Framing strategy: records are delimited by a newline. This matches how the
// device data is stored in the .NET ReceivedData logs and how our simulator
// replays it. NOTE: the true on-the-wire terminator must be confirmed against
// the VTS_GPS_L100 device spec before production; if devices do not send a
// newline, this split function is the single place to change.

// NewFrameScanner wraps a reader with the packet split function and a bounded
// buffer. maxFrame caps a single packet so a misbehaving or malicious device
// cannot make us buffer without limit.
func NewFrameScanner(r interface {
	Read([]byte) (int, error)
}, maxFrame int) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 4096), maxFrame)
	s.Split(SplitPackets)
	return s
}

// SplitPackets is a bufio.SplitFunc. Contract:
//   - return (0, nil, nil)         -> "need more data", scanner reads again
//   - return (advance, token, nil) -> one complete frame of `advance` bytes
//
// It splits on '\n', tolerates '\r\n', and skips blank frames. At EOF it
// returns any trailing bytes that were not newline-terminated (the device's
// last packet may lack a trailing newline).
func SplitPackets(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		frame := dropCR(data[:i])
		if len(bytes.TrimSpace(frame)) == 0 {
			// Blank line between packets: consume it, ask for the next.
			return i + 1, nil, nil
		}
		return i + 1, frame, nil
	}

	if atEOF {
		frame := dropCR(data)
		if len(bytes.TrimSpace(frame)) == 0 {
			return len(data), nil, nil
		}
		return len(data), frame, nil
	}

	// No newline yet and not at EOF: buffer and wait for more bytes.
	return 0, nil, nil
}

func dropCR(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\r' {
		return b[:len(b)-1]
	}
	return b
}
