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
// Framing strategy: two delimiters are recognised, whichever appears first.
//
//  1. ON THE WIRE (real devices). Confirmed by packet capture from IMEI
//     862360075287248 on 2026-08-01, every frame looks like:
//
//     0x20? 0x01 "ATL" <15-digit IMEI> "," "$GPRMC" ... <payload> "ATL" 0x02 <1 byte>
//
//     so the terminator is the literal "ATL" followed by 0x02, plus one
//     trailing byte (a counter or checksum; it varied 0x40/0x41 across frames
//     and is not interpreted here). Devices send NO newline.
//
//     The .NET CustomReceiveFilter had no delimiter at all -- it assumed one
//     socket read == one packet -- which is finding D5. There was therefore
//     never an on-the-wire newline to copy; the earlier assumption came from
//     the ReceivedData *log files*, which are newline-separated after the fact.
//
//  2. NEWLINE. Kept so the simulator and ReceivedData log replays still work.
//
// The emitted frame EXCLUDES the trailing "ATL" 0x02 <byte>. Leaving it on
// would concatenate the marker onto the last comma-separated field (the cell
// id), corrupting it -- the .NET code has that flaw. The leading 0x01"ATL" IS
// retained because extractIMEI, mirroring the .NET Substring(4, 15), counts on
// those four bytes being present.

// frameEnd is the on-the-wire terminator: the literal "ATL" then 0x02. One
// further byte follows it and is consumed but not emitted. "ATL" alone is not
// enough -- payloads legitimately contain "ATLTPMS" and similar.
var frameEnd = []byte("ATL\x02")

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

	// Whichever delimiter comes first wins, so a stream that mixes the two
	// (a log replay piped at a live listener, say) still frames correctly.
	end := bytes.Index(data, frameEnd)
	nl := bytes.IndexByte(data, '\n')

	if end >= 0 && (nl < 0 || end < nl) {
		// One trailing byte follows the 0x02 and belongs to this frame.
		advance := end + len(frameEnd) + 1
		if advance > len(data) {
			if !atEOF {
				return 0, nil, nil // trailer byte has not arrived yet
			}
			advance = len(data) // truncated final frame; take what we have
		}
		frame := data[:end]
		if len(bytes.TrimSpace(frame)) == 0 {
			return advance, nil, nil
		}
		return advance, frame, nil
	}

	if i := nl; i >= 0 {
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
