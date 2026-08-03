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
//     0x20 <ctrl> "ATL" <15-digit IMEI> "," "$GPRMC" ... <payload> "ATL" <ctrl+1> <byte>
//
//     Both the head and tail control bytes VARY -- observed head 0x01 and 0x03
//     with matching tails 0x02 and 0x04 -- so neither can be hard-coded. What
//     is invariant is that the closing "ATL" is followed by a byte below 0x20,
//     while the opening one is followed by the IMEI's digits. See
//     indexFrameEnd. Devices send NO newline.
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

// atl is the marker that both opens and closes a frame. Which one it is depends
// entirely on the byte that follows it:
//
//	"ATL" + digits        -> frame START ("ATL862360075287248,$GPRMC,...")
//	"ATL" + control byte  -> frame END   (trailer: 1 control byte + 1 ASCII byte)
//
// Verified over a 6-packet TCP segment from device 862360075287248: every one
// of the 12 "ATL" occurrences was followed by either "86" (the IMEI) or a byte
// below 0x20. Matching on "ATL" alone would false-positive on payload content
// such as "ATLTPMS".
var atl = []byte("ATL")

// indexFrameEnd returns the offset of the frame terminator in data, or -1 if
// there is not yet enough data to identify one.
//
// Returning -1 for "ATL" sitting at the very end of the buffer is deliberate:
// we cannot yet tell a terminator from the start of the next frame, so the
// caller must read more bytes rather than guess.
func indexFrameEnd(data []byte) int {
	for from := 0; ; {
		i := bytes.Index(data[from:], atl)
		if i < 0 {
			return -1
		}
		i += from
		if i+len(atl) >= len(data) {
			return -1 // next byte not read yet -- undecidable
		}
		if data[i+len(atl)] < 0x20 {
			return i
		}
		from = i + 1 // a start marker; keep looking
	}
}

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
	end := indexFrameEnd(data)
	nl := bytes.IndexByte(data, '\n')

	if end >= 0 && (nl < 0 || end < nl) {
		// Trailer is "ATL" + 1 control byte + 1 ASCII byte.
		advance := end + len(atl) + 2
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
