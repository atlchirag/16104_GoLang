// Command simulator pretends to be a GPS device: it opens a TCP connection to
// the listener, sends packets (each terminated with a newline, matching the
// framing), and reads the 0xAA acknowledgement. Use it to drive the pipeline
// end to end without real hardware.
//
// Usage:
//
//	simulator                         # send one built-in real packet
//	simulator -addr host:16104 -n 5   # send it 5 times
//	simulator -file packets.txt       # replay a capture, one packet per line
//
// Flags via environment too: SIM_ADDR overrides -addr.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// builtinPacket is the verbatim capture used across the tests.
const builtinPacket = `LATL864180055638907,$GPRMC,071334,A,3041.0149,N,07649.0098,E,0.0,0,270526,,,*2E,#01111011000001,0.00,-70.00,0,7335.60,33,4.2,31,404,2,1ef6,7b38c19,99,1354,ATLTPMS,@60A423EF0726::02010609FF160F012403222A11000000000000000000000000000000000000@ATLLm`

func main() {
	addr := flag.String("addr", envOr("SIM_ADDR", "localhost:16104"), "listener address host:port")
	file := flag.String("file", "", "file of packets to replay (one per line); default sends the built-in packet")
	n := flag.Int("n", 1, "number of times to send when using the built-in packet")
	delay := flag.Duration("delay", 200*time.Millisecond, "delay between packets")
	date := flag.String("date", "today", `GPRMC date stamp to send: "today", "keep" (use the capture's own date), or an explicit ddMMyy such as 010826`)
	flag.Parse()

	packets, err := loadPackets(*file, *n)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load packets:", err)
		os.Exit(1)
	}

	// The capture was recorded on 27/05/26, and pgwriter picks the daily table
	// from the packet's own date -- so replaying it verbatim writes into
	// tbl_telemetry_27052026, not today's table (which is the only one the
	// scheduler has created). Retarget it by default.
	if *date != "keep" {
		stamp := *date
		if stamp == "today" {
			stamp = time.Now().Format("020106") // ddMMyy
		}
		for i := range packets {
			packets[i] = retargetDate(packets[i], stamp)
		}
		fmt.Printf("date stamp set to %s\n", stamp)
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("connected to %s, sending %d packet(s)\n", *addr, len(packets))

	reader := bufio.NewReader(conn)
	for i, p := range packets {
		if _, err := conn.Write([]byte(p + "\n")); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
		// Read the single-byte 0xAA ack.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, err := reader.ReadByte()
		if err != nil {
			fmt.Fprintf(os.Stderr, "packet %d: no ack: %v\n", i+1, err)
		} else {
			fmt.Printf("packet %d sent, ack=0x%02X\n", i+1, b)
		}
		if i < len(packets)-1 {
			time.Sleep(*delay)
		}
	}
	fmt.Println("done")
}

func loadPackets(file string, n int) ([]string, error) {
	if file == "" {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, builtinPacket)
		}
		return out, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		// Skip the .NET log's timestamp lines (e.g. "5/27/2026 1:04:16 PM").
		if strings.Contains(line, "$GPRMC") {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no $GPRMC packets found in %s", file)
	}
	return out, nil
}

// retargetDate rewrites the GPRMC date stamp (ddMMyy) in one packet. That field
// is f[9] after the "$GPRMC" marker -- the same index ParseGPRMC reads -- and it
// decides which monthly table pgwriter writes to. Nothing verifies the NMEA
// checksum (neither the .NET service nor our parser), so editing the field in
// place leaves the packet perfectly acceptable.
//
// Packets without a $GPRMC marker, or with too few fields, are returned
// untouched: the pipeline should get its own chance to reject them.
func retargetDate(packet, ddMMyy string) string {
	idx := strings.Index(packet, "$GPRMC")
	if idx < 0 {
		return packet
	}
	head := packet[:idx+len("$GPRMC")]
	f := strings.Split(packet[idx+len("$GPRMC"):], ",")
	if len(f) < 10 {
		return packet
	}
	f[9] = ddMMyy
	return head + strings.Join(f, ",")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
