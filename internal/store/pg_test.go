package store

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsPermanentUndefinedTable is the regression test for the outage of
// 2026-08-18: a device reporting a 2001 timestamp made tableFor select a daily
// table that did not exist, and because 42P01 was retried like a transient
// error the consumer stalled its partition for hours.
func TestIsPermanentUndefinedTable(t *testing.T) {
	err := &pgconn.PgError{
		Code:    "42P01",
		Message: `relation "tbl_telemetry_14122001" does not exist`,
	}

	condition, permanent := IsPermanent(err)
	if !permanent {
		t.Fatal("42P01 must be permanent, otherwise the batch retries forever and blocks the partition")
	}
	if condition != "undefined_table" {
		t.Errorf("condition = %q, want %q", condition, "undefined_table")
	}
}

func TestIsPermanentClassification(t *testing.T) {
	tests := []struct {
		name      string
		code      string
		permanent bool
	}{
		{"undefined table", "42P01", true},
		{"undefined column", "42703", true},
		{"value too long for varchar", "22001", true},
		{"datetime field overflow", "22008", true},
		{"not null violation", "23502", true},

		// These recover precisely because they are retried. Classifying any of
		// them as permanent would silently divert good data to the DLQ.
		{"serialization failure", "40001", false},
		{"deadlock detected", "40P01", false},
		{"too many connections", "53300", false},
		{"query canceled", "57014", false},
		{"admin shutdown", "57P01", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := IsPermanent(&pgconn.PgError{Code: tc.code})
			if got != tc.permanent {
				t.Errorf("IsPermanent(%s) = %v, want %v", tc.code, got, tc.permanent)
			}
		})
	}
}

// A non-Postgres error (a dial failure, a context deadline) carries no SQLSTATE
// and must stay retryable.
func TestIsPermanentNonPgError(t *testing.T) {
	if _, permanent := IsPermanent(errors.New("dial tcp: connection refused")); permanent {
		t.Error("a plain error must not be treated as permanent")
	}
}

// The classifier has to see through the wrapping that pgx and our own code add.
func TestIsPermanentWrapped(t *testing.T) {
	wrapped := fmt.Errorf("insert telemetry: %w",
		&pgconn.PgError{Code: "42P01", Message: "nope"})

	if _, permanent := IsPermanent(wrapped); !permanent {
		t.Error("IsPermanent must unwrap; errors.As, not a type assertion")
	}
}

func TestErrUnknownIMEIIsIdentifiable(t *testing.T) {
	wrapped := fmt.Errorf("%w: %s", ErrUnknownIMEI, "862360075287248")
	if !errors.Is(wrapped, ErrUnknownIMEI) {
		t.Error("callers must be able to distinguish an unknown device from a database outage")
	}
}

// tableFor is what turned a bad device clock into an outage, so pin its
// behaviour: the +330 offset decides which local day a UTC instant belongs to.
func TestTableFor(t *testing.T) {
	s := &Store{utcOffsetMin: 330, tableMode: TableDaily}

	tests := []struct {
		name    string
		gpsTime time.Time
		want    string
	}{
		{
			name:    "midday UTC stays on the same local day",
			gpsTime: time.Date(2026, 8, 18, 7, 42, 12, 0, time.UTC),
			want:    "tbl_telemetry_18082026",
		},
		{
			name:    "late UTC evening rolls into the next local day",
			gpsTime: time.Date(2026, 8, 18, 20, 0, 0, 0, time.UTC),
			want:    "tbl_telemetry_19082026",
		},
		{
			name:    "the 2001 packet that caused the outage",
			gpsTime: time.Date(2001, 12, 13, 20, 48, 44, 0, time.UTC),
			want:    "tbl_telemetry_14122001",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.tableFor(tc.gpsTime); got != tc.want {
				t.Errorf("tableFor() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestTableForMonthly pins the monthly naming this port can be switched back to
// with TELEMETRY_TABLE_MODE=monthly. The suffix must match what the .NET service
// produced -- ToString("MMMyy").ToLower() -- because those are real tables that
// already exist and already hold data.
func TestTableForMonthly(t *testing.T) {
	s := &Store{utcOffsetMin: 330, tableMode: TableMonthly}

	for _, tc := range []struct {
		name    string
		gpsTime time.Time
		want    string
	}{
		{
			name:    "midday UTC",
			gpsTime: time.Date(2026, 9, 25, 6, 0, 0, 0, time.UTC),
			want:    "tbl_telemetry_sep26",
		},
		{
			// The +330 shift can move a packet into the NEXT month, which is
			// exactly when a missing monthly table bites: nothing creates them.
			name:    "last day of the month, late UTC, rolls into the next month",
			gpsTime: time.Date(2026, 9, 30, 20, 0, 0, 0, time.UTC),
			want:    "tbl_telemetry_oct26",
		},
		{
			name:    "single-digit month is not zero padded",
			gpsTime: time.Date(2026, 8, 18, 7, 42, 12, 0, time.UTC),
			want:    "tbl_telemetry_aug26",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.tableFor(tc.gpsTime); got != tc.want {
				t.Errorf("tableFor() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestTableModeDefaultsToDaily guards the switch: an unset or misspelt
// TELEMETRY_TABLE_MODE must fall back to daily, never silently to monthly.
func TestTableModeDefaultsToDaily(t *testing.T) {
	for _, mode := range []string{"", "Daily", "MONTHLY", "montly", "weekly"} {
		s := &Store{utcOffsetMin: 330, tableMode: mode}
		if mode != TableMonthly {
			s.tableMode = TableDaily // what New() does for anything unrecognised
		}
		got := s.tableFor(time.Date(2026, 9, 25, 6, 0, 0, 0, time.UTC))
		if mode == TableMonthly {
			continue
		}
		if got != "tbl_telemetry_25092026" {
			t.Errorf("mode %q gave %q, want the daily name", mode, got)
		}
	}
}
