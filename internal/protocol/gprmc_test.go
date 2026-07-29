package protocol

import (
	"math"
	"testing"
)

// realPacket is a verbatim capture from
// E:\ListenerData\AtlantaNew\16104\27052026\ReceivedData\864180055638907.txt
const realPacket = `LATL864180055638907,$GPRMC,071334,A,3041.0149,N,07649.0098,E,0.0,0,270526,,,*2E,#01111011000001,0.00,-70.00,0,7335.60,33,4.2,31,404,2,1ef6,7b38c19,99,1354,ATLTPMS,@60A423EF0726::02010609FF160F012403222A11000000000000000000000000000000000000@ATLLm`

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestParseGPRMC_RealPacket(t *testing.T) {
	msg, err := ParseGPRMC(realPacket)
	if err != nil {
		t.Fatalf("ParseGPRMC returned error: %v", err)
	}

	if msg.IMEI != "864180055638907" {
		t.Errorf("IMEI = %q, want 864180055638907", msg.IMEI)
	}

	// gps_time: date 270526 (DDMMYY) + time 071334 (HHMMSS) -> 2026-05-27 07:13:34 UTC
	if got := msg.GPSTime.Format("2006-01-02 15:04:05"); got != "2026-05-27 07:13:34" {
		t.Errorf("GPSTime = %s, want 2026-05-27 07:13:34", got)
	}

	if !msg.Position.Valid {
		t.Error("Position.Valid = false, want true (validity 'A')")
	}
	// 3041.0149 N -> 30 + 41.0149/60
	if wantLat := 30.0 + 41.0149/60.0; !approx(msg.Position.Latitude, wantLat) {
		t.Errorf("Latitude = %v, want %v", msg.Position.Latitude, wantLat)
	}
	// 07649.0098 E -> 76 + 49.0098/60
	if wantLon := 76.0 + 49.0098/60.0; !approx(msg.Position.Longitude, wantLon) {
		t.Errorf("Longitude = %v, want %v", msg.Position.Longitude, wantLon)
	}
	if !approx(msg.Position.SpeedKmh, 0.0) {
		t.Errorf("SpeedKmh = %v, want 0", msg.Position.SpeedKmh)
	}

	// I/O string "#01111011000001": positions 1..14
	io := msg.IO
	if io.I2Ignition != 0 || io.I8Door != 1 || io.I3SOS != 1 || io.I1MainPower != 1 || io.I14Accelerometer != 1 {
		t.Errorf("IO decode mismatch: %+v", io)
	}

	if !approx(msg.Vehicle.Odometer, 7335.60) {
		t.Errorf("Odometer = %v, want 7335.60", msg.Vehicle.Odometer)
	}

	if msg.Network.MobileCountryCode != "404" {
		t.Errorf("MCC = %q, want 404", msg.Network.MobileCountryCode)
	}
	if msg.Network.CellID != "7b38c19" {
		t.Errorf("CellID = %q, want 7b38c19", msg.Network.CellID)
	}
	if msg.Network.SignalStrength == nil || *msg.Network.SignalStrength != 31 {
		t.Errorf("SignalStrength = %v, want 31", msg.Network.SignalStrength)
	}
}

func TestParseGPRMC_NotGPRMC(t *testing.T) {
	if _, err := ParseGPRMC("$CMD|5930|$MSG,GETGPS<6906>&"); err != ErrNotGPRMC {
		t.Errorf("err = %v, want ErrNotGPRMC", err)
	}
}
