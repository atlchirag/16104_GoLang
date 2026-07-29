package protocol

import (
	"bytes"
	"io"
	"testing"
)

// These tests prove the D5 fix: the same three packets must be framed
// identically whether the bytes arrive all at once, one byte at a time, or in
// arbitrary chunks. This is a Phase 2 exit criterion in the plan.

var framingInput = []byte(
	"LATL111111111111111,$GPRMC,010101,A,3041.0149,N,07649.0098,E,0.0,0,270526,,,*2E,#010000000000000,0,0,0,0,33,4.2,31,404,2,1ef6,aaa,99,1,ATL,@x\n" +
		"LATL222222222222222,$GPRMC,020202,A,2838.0356,N,07713.3750,E,0.0,0,200326,,,*25,#010000000000000,0,0,0,0,33,4.2,31,404,2,1ef6,bbb,99,1,ATL,@y\n" +
		"LATL333333333333333,$GPRMC,030303,A,3101.8994,N,07637.9658,E,0.0,0,250526,,,*2E,#010000000000000,0,0,0,0,33,4.2,31,404,2,1ef6,ccc,99,1,ATL,@z\n")

func framesFrom(t *testing.T, chunkSize int) [][]byte {
	t.Helper()
	r := &chunkReader{data: framingInput, chunk: chunkSize}
	s := NewFrameScanner(r, 64*1024)
	var out [][]byte
	for s.Scan() {
		b := append([]byte(nil), s.Bytes()...)
		out = append(out, b)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("scanner error (chunk=%d): %v", chunkSize, err)
	}
	return out
}

func TestFraming_DeliveryInvariant(t *testing.T) {
	whole := framesFrom(t, 1_000_000) // effectively one read
	if len(whole) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(whole))
	}

	for _, cs := range []int{1, 2, 7, 64, 200} {
		got := framesFrom(t, cs)
		if len(got) != len(whole) {
			t.Fatalf("chunk=%d: frame count %d != %d", cs, len(got), len(whole))
		}
		for i := range got {
			if !bytes.Equal(got[i], whole[i]) {
				t.Fatalf("chunk=%d: frame %d differs\n got: %s\nwant: %s", cs, i, got[i], whole[i])
			}
		}
	}
}

// chunkReader hands out data in fixed-size pieces to simulate TCP delivering a
// stream in arbitrary fragments.
type chunkReader struct {
	data  []byte
	chunk int
	pos   int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := c.chunk
	if n > len(p) {
		n = len(p)
	}
	if c.pos+n > len(c.data) {
		n = len(c.data) - c.pos
	}
	copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}
