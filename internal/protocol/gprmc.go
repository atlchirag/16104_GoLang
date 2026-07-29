package protocol

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/wiziot/gps-platform/internal/model"
)

// This is a faithful port of the AtlantaNew GPRMC parsing in
// AtlantaNewServer.InsertIntoDB, plus the GetLatitude/GetLongitude/
// GetGPSDateTime helpers from General.cs. It is intentionally pure: bytes in,
// struct out, no database and no file writes. That is what makes the
// golden-file parity tests (plan section 8.2) possible.
//
// Not yet ported (next increment): BLE sensor sections, WPT100 (ATLTPMS)
// overrides, the L400 "fuel@temp@volt" composite, and $LOC location handling.
// Their field positions are noted below so extending is mechanical.

var (
	// ErrNotGPRMC means the packet has no $GPRMC sentence (e.g. a command
	// response or a heartbeat). The caller decides what to do with it.
	ErrNotGPRMC = errors.New("packet contains no $GPRMC sentence")
	// ErrShortPacket means the $GPRMC field list is too short to parse.
	ErrShortPacket = errors.New("gprmc field list too short")
	// ErrBadGPSTime means the date/time stamp did not form a valid timestamp.
	ErrBadGPSTime = errors.New("invalid gps date/time")
)

// ParseGPRMC turns one raw AtlantaNew packet into a TelemetryMessage. It fills
// IMEI, GPS fix, I/O flags, odometer and network fields. utcOffsetMinutes is
// used only where the .NET code used session.UTCOffset; the thin slice does not
// yet depend on it, so 0 is fine.
func ParseGPRMC(raw string) (*model.TelemetryMessage, error) {
	// Split on "$GPRMC" exactly like CustomReceiveFilter/InsertIntoDB.
	idx := strings.Index(raw, "$GPRMC")
	if idx < 0 {
		return nil, ErrNotGPRMC
	}
	prefix := raw[:idx]                // e.g. "LATL864180055638907,"
	segment := raw[idx+len("$GPRMC"):] // ",071334,A,3041.0149,N,..."

	imei := extractIMEI(prefix)

	f := strings.Split(segment, ",")
	// Faithful to .NET: f[0] is the empty string before the first comma.
	// We need up to index 24 (+locationFlag) for the network fields.
	if len(f) < 18 {
		return nil, ErrShortPacket
	}

	msg := &model.TelemetryMessage{
		SchemaVersion: model.SchemaVersion,
		IMEI:          imei,
		ReceivedAt:    time.Now().UTC(),
		RawPacket:     raw,
		Sensors:       []model.Sensor{},
	}

	// --- GPS time (General.GetGPSDateTime(dateStamp=f[9], timeStamp=f[1])) ---
	gpsTime, err := getGPSDateTime(field(f, 9), field(f, 1))
	if err != nil {
		return nil, ErrBadGPSTime
	}
	msg.GPSTime = gpsTime

	// --- Position ---
	msg.Position.Valid = field(f, 2) == "A"
	lat := getLatitude(field(f, 3))
	latDir := field(f, 4)
	if latDir == "S" {
		lat = -lat
	}
	lon := getLongitude(field(f, 5))
	lonDir := field(f, 6)
	if lonDir == "W" {
		// NOTE: the .NET code has a bug here (it negates latitude, not
		// longitude). We do the correct thing and negate longitude. This is a
		// documented, intentional correction per plan section 8.2. No test
		// packet currently exercises the western hemisphere.
		lon = -lon
	}
	msg.Position.Latitude = lat
	msg.Position.Longitude = lon
	msg.Position.LatDir = latDir
	msg.Position.LonDir = lonDir

	if v := field(f, 7); v != "" {
		if knots, e := strconv.ParseFloat(v, 64); e == nil {
			msg.Position.SpeedKmh = knots * 1.852
		}
	}
	if v := field(f, 8); v != "" {
		if course, e := strconv.ParseFloat(v, 64); e == nil {
			msg.Position.Orientation = course
		}
	}
	msg.Position.Variation = parseFloatPtr(field(f, 10))

	// --- $LOC handling stub -------------------------------------------------
	// In .NET, if f[12] (checksum) contains "$LOC" then f[13] is the address
	// and every field after shifts by one (locationFlag). We do not yet parse
	// that; locationFlag stays 0. Add it with the BLE increment.
	locationFlag := 0

	// --- I/O flag string: f[13+locationFlag], leading '#' skipped ---
	msg.IO = parseIOFlags(field(f, 13+locationFlag))

	// --- Vehicle: odometer only for now; fuel/temp come from BLE/L400 later --
	msg.Vehicle.Odometer = parseFloat(field(f, 17+locationFlag))
	if v := field(f, 16+locationFlag); v != "" && !strings.Contains(v, "@") {
		msg.Vehicle.TankCapacity = parseFloatPtr(v)
	}
	msg.Vehicle.BatteryVoltage = parseFloatPtr(field(f, 19+locationFlag))

	// --- Network fields -----------------------------------------------------
	msg.Network.FirmwareVersion = field(f, 18+locationFlag)
	msg.Network.SignalStrength = parseIntPtr(field(f, 20+locationFlag))
	msg.Network.MobileCountryCode = field(f, 21+locationFlag)
	msg.Network.MobileNetworkCode = field(f, 22+locationFlag)
	msg.Network.LocationAreaCode = field(f, 23+locationFlag)
	msg.Network.CellID = field(f, 24+locationFlag)

	return msg, nil
}

// PeekIMEI extracts the IMEI from a raw frame without fully parsing it. Used to
// key the gps.raw record so raw bytes are retained even when parsing fails.
func PeekIMEI(raw string) string {
	prefix := raw
	if idx := strings.Index(raw, "$GPRMC"); idx >= 0 {
		prefix = raw[:idx]
	}
	return extractIMEI(prefix)
}

// extractIMEI mirrors CustomReceiveFilter: Key = data[0].Substring(4, 15).
// prefix is everything before "$GPRMC", e.g. "LATL864180055638907,".
func extractIMEI(prefix string) string {
	p := strings.TrimRight(strings.TrimSpace(prefix), ",")
	if len(p) >= 19 {
		return p[4:19] // skip "LATL", take 15 chars
	}
	// Fallback: pull the longest digit run.
	var b strings.Builder
	for _, c := range p {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// parseIOFlags decodes the "#01111011000001" string. Index 0 is '#'; the .NET
// code reads positions 1..14. Any missing/short input yields zeros.
func parseIOFlags(s string) model.IO {
	c := func(i int) int {
		if i < len(s) && s[i] >= '0' && s[i] <= '9' {
			return int(s[i] - '0')
		}
		return 0
	}
	return model.IO{
		I2Ignition:       c(1),
		I8Door:           c(2),
		I3SOS:            c(3),
		I4:               c(4),
		I5:               c(5),
		I6AC:             c(6),
		I7:               c(7),
		I1MainPower:      c(8),
		I9HarshSpeeding:  c(9),
		I10HarshBraking:  c(10),
		I11ArmDisarm:     c(11),
		I12Sleep:         c(12),
		I13Relay:         c(13),
		I14Accelerometer: c(14),
	}
}

// getLatitude ports General.GetLatitude: ddmm.mmmm -> decimal degrees.
func getLatitude(latitude string) float64 {
	if len(latitude) <= 4 {
		return 0
	}
	parts := strings.SplitN(latitude, ".", 2)
	if len(parts) < 2 {
		return 0
	}
	// Left-pad the integer part to at least 4 chars, matching .NET.
	for len(parts[0]) < 4 {
		parts[0] = "0" + parts[0]
	}
	deg := parts[0]
	minutes, err := strconv.ParseFloat(deg[len(deg)-2:]+"."+parts[1], 64)
	if err != nil {
		return 0
	}
	whole, err := strconv.ParseFloat(deg[:len(deg)-2], 64)
	if err != nil {
		return 0
	}
	return whole + minutes/60.0
}

// getLongitude ports General.GetLongitude: dddmm.mmmm -> decimal degrees.
func getLongitude(longitude string) float64 {
	if len(longitude) <= 4 {
		return 0
	}
	parts := strings.SplitN(longitude, ".", 2)
	if len(parts) < 2 {
		return 0
	}
	deg := parts[0]
	minutes, err := strconv.ParseFloat(deg[len(deg)-2:]+"."+parts[1], 64)
	if err != nil {
		return 0
	}
	whole, err := strconv.ParseFloat(deg[:len(deg)-2], 64)
	if err != nil {
		return 0
	}
	return whole + minutes/60.0
}

// getGPSDateTime ports General.GetGPSDateTime: dateStamp DDMMYY + timeStamp
// HHMMSS -> time. Returns an error instead of the .NET "default DateTime".
func getGPSDateTime(dateStamp, timeStamp string) (time.Time, error) {
	if len(dateStamp) < 6 || len(timeStamp) < 6 {
		return time.Time{}, ErrBadGPSTime
	}
	year, e1 := strconv.Atoi("20" + dateStamp[4:6])
	month, e2 := strconv.Atoi(dateStamp[2:4])
	day, e3 := strconv.Atoi(dateStamp[0:2])
	hh, e4 := strconv.Atoi(timeStamp[0:2])
	mm, e5 := strconv.Atoi(timeStamp[2:4])
	ss, e6 := strconv.Atoi(timeStamp[4:6])
	if err := firstErr(e1, e2, e3, e4, e5, e6); err != nil {
		return time.Time{}, ErrBadGPSTime
	}
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, ErrBadGPSTime
	}
	// The device reports GPS (UTC) time; we keep it in UTC.
	return time.Date(year, time.Month(month), day, hh, mm, ss, 0, time.UTC), nil
}

// --- small helpers ----------------------------------------------------------

func field(f []string, i int) string {
	if i >= 0 && i < len(f) {
		return strings.TrimSpace(f[i])
	}
	return ""
}

func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseFloatPtr(s string) *float64 {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}

func parseIntPtr(s string) *int64 {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
