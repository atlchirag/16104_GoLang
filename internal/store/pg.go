// Package store is the consumer-side PostgreSQL access layer. All writes use
// pgx with parameterised statements (never string concatenation), which closes
// finding D6, and every telemetry insert is idempotent so at-least-once Kafka
// redelivery cannot create duplicate rows (plan section 4.6.2).
package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wiziot/gps-platform/internal/model"
)

// Table naming schemes for the telemetry table.
//
// The pipeline moved from monthly to daily tables in August 2026 (see tableFor).
// TableMonthly exists so this port can be put back on monthly tables without a
// rebuild -- it is the only port in the family that can. Everything else writes
// daily, unconditionally.
const (
	TableDaily   = "daily"
	TableMonthly = "monthly"
)

// Store owns the connection pool and the IMEI -> service_id cache.
type Store struct {
	pool         *pgxpool.Pool
	utcOffsetMin int
	tableMode    string

	mu    sync.RWMutex
	cache map[string]cacheEntry // imei -> service_id
}

type cacheEntry struct {
	serviceID int64
	expires   time.Time
}

const cacheTTL = 5 * time.Minute

// New opens a pgx pool. utcOffsetMin is added to gps_time when choosing the
// table name, carrying over the .NET tableMonth offset (India = +330).
// tableMode is TableDaily or TableMonthly; anything else is treated as daily.
func New(ctx context.Context, dsn string, utcOffsetMin int, tableMode string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if tableMode != TableMonthly {
		tableMode = TableDaily
	}
	return &Store{
		pool:         pool,
		utcOffsetMin: utcOffsetMin,
		tableMode:    tableMode,
		cache:        map[string]cacheEntry{},
	}, nil
}

// TableMode reports which naming scheme is in force, for start-up logging.
func (s *Store) TableMode() string {
	return s.tableMode
}

func (s *Store) Close() { s.pool.Close() }

// ResolveServiceID maps an IMEI to its service id, caching the result with a
// TTL. This replaces the per-packet SELECT the .NET listener did on the hot
// path (finding D2): configuration is read rarely, not at packet rate.
func (s *Store) ResolveServiceID(ctx context.Context, imei string) (int64, error) {
	s.mu.RLock()
	if e, ok := s.cache[imei]; ok && time.Now().Before(e.expires) {
		s.mu.RUnlock()
		return e.serviceID, nil
	}
	s.mu.RUnlock()

	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT s.id
		   FROM tbl_devices d
		   JOIN tbl_services s ON s.sys_device_id = d.id
		  WHERE d.imei = $1`, imei).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Distinguished from a database outage so the caller can divert the
		// record instead of skipping it silently, which lost it outright.
		return 0, fmt.Errorf("%w: %s", ErrUnknownIMEI, imei)
	}
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	s.cache[imei] = cacheEntry{serviceID: id, expires: time.Now().Add(cacheTTL)}
	s.mu.Unlock()
	return id, nil
}

// tableFor returns the telemetry table name for a packet's timestamp, in
// whichever naming scheme is configured.
//
//	daily    tbl_telemetry_17082026   ddmmyyyy   (the default, and what every
//	                                              other port in the family uses)
//	monthly  tbl_telemetry_aug26      mmmyy      (what the .NET service wrote,
//	                                              and what this port used until
//	                                              August 2026)
//
// Both derive the suffix from gps_time shifted by utcOffsetMin, matching the
// .NET `tableMonth` expression.
//
// WHO CREATES THE TABLE differs between the two, and that is the thing that
// bites:
//
//   - daily: the daily-telemetry service creates tomorrow's table at 23:55 IST
//     from the same +330 offset. The two MUST agree or this insert targets a
//     table nobody created.
//   - monthly: NOTHING in this platform creates monthly tables any more; they
//     are leftovers from the SQL Server era. Before switching to monthly,
//     confirm the target month's table exists AND carries a unique index over
//     (gps_time, sys_service_id) -- without it ON CONFLICT fails with 42P10 and
//     every row is diverted to the DLQ.
func (s *Store) tableFor(gpsTime time.Time) string {
	local := gpsTime.Add(time.Duration(s.utcOffsetMin) * time.Minute)
	if s.tableMode == TableMonthly {
		// .NET wrote ToString("MMMyy").ToLower(), e.g. "sep26".
		return "tbl_telemetry_" + strings.ToLower(local.Format("Jan06"))
	}
	return "tbl_telemetry_" + local.Format("02012006")
}

// InsertTelemetry writes one telemetry row idempotently. serviceID must already
// be resolved. rawJSON is the full TelemetryMessage as jsonb.
//
// The I-column mapping is faithful to AtlantaNewServer.InsertIntoDB, including
// its quirks: I6 is a literal 0 and the A/C flag lands in I15.
func (s *Store) InsertTelemetry(ctx context.Context, m *model.TelemetryMessage, serviceID int64, rawJSON []byte) error {
	table := s.tableFor(m.GPSTime)

	// Identifiers cannot be parameters in SQL, so the table name is
	// interpolated. It is derived from a timestamp, never from device input,
	// so there is no injection surface. All values below are parameters.
	sql := fmt.Sprintf(`
		INSERT INTO %s (
			sys_service_id, sys_proc_time, gps_time, gps_validity,
			gps_latitude, latitude_direction, gps_longitude, longitude_direction,
			gps_speed, gps_orientation, variation, checksum,
			i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
			tel_odometer, battery_voltage, signal_strength,
			mobile_country_code, mobile_network_code, location_area_code, cell_id,
			firmware_version, raw_data
		) VALUES (
			$1, now(), $2, $3,
			$4, $5, $6, $7,
			$8, $9, $10, $11,
			$12, $13, $14, $15, $16, 0, $17, $18, $19, $20, $21, $22, $23, $24, $25,
			$26, $27, $28,
			$29, $30, $31, $32,
			$33, $34::jsonb
		)
		ON CONFLICT (sys_service_id, gps_time) DO NOTHING`, table)

	validity := "V"
	if m.Position.Valid {
		validity = "A"
	}

	_, err := s.pool.Exec(ctx, sql,
		serviceID, m.GPSTime, validity,
		m.Position.Latitude, m.Position.LatDir, m.Position.Longitude, m.Position.LonDir,
		m.Position.SpeedKmh, m.Position.Orientation, numStr(m.Position.Variation), "",
		m.IO.I1MainPower, m.IO.I2Ignition, m.IO.I3SOS, m.IO.I4, m.IO.I5, /*i6 literal 0*/
		m.IO.I7, m.IO.I8Door, m.IO.I9HarshSpeeding, m.IO.I10HarshBraking, m.IO.I11ArmDisarm,
		m.IO.I12Sleep, m.IO.I13Relay, m.IO.I14Accelerometer, m.IO.I6AC, /* -> i15 */
		m.Vehicle.Odometer, numStr(m.Vehicle.BatteryVoltage), intStr(m.Network.SignalStrength),
		m.Network.MobileCountryCode, m.Network.MobileNetworkCode, m.Network.LocationAreaCode, m.Network.CellID,
		m.Network.FirmwareVersion, string(rawJSON),
	)
	return err
}

// permanentCodes are SQLSTATEs where retrying the identical statement can only
// fail the identical way. Retrying them forever blocks the Kafka partition
// behind the offending record, so the caller must divert the record instead.
//
// 42P01 is the one that actually bit us in production: a device whose clock had
// slipped to 2001 made tableFor pick tbl_telemetry_14122001, a daily table that
// nobody had created, and the writer retried that batch for hours while every
// packet behind it — including the well-dated ones — waited.
//
// Deliberately absent: connection failures, 40001 serialization_failure, 40P01
// deadlock_detected, 53300 too_many_connections, 57014 query_canceled. Those
// are transient and MUST keep their retry, since retrying is what makes them
// recover.
var permanentCodes = map[string]string{
	"42P01": "undefined_table",
	"42703": "undefined_column",
	"42804": "datatype_mismatch",
	"42883": "undefined_function",
	"22001": "string_data_right_truncation",
	"22003": "numeric_value_out_of_range",
	"22007": "invalid_datetime_format",
	"22008": "datetime_field_overflow",
	"22P02": "invalid_text_representation",
	"23502": "not_null_violation",
	"23503": "foreign_key_violation",
	"23514": "check_violation",
}

// IsPermanent reports whether err is a Postgres error that will recur on every
// retry of the same row, and returns the condition name for logging.
func IsPermanent(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}
	name, ok := permanentCodes[pgErr.Code]
	return name, ok
}

// ErrUnknownIMEI reports that no service row exists for the device. It is a
// permanent condition for the record: the row cannot be written without a
// sys_service_id, and no amount of redelivery invents one.
var ErrUnknownIMEI = errors.New("no service registered for imei")

// numStr and intStr render a numeric value as text, keeping nil as SQL NULL.
//
// variation, battery_voltage and signal_strength are numeric in our model but
// varchar(500) in the database, because the .NET service wrote them with string
// concatenation. pgx refuses to encode a float64 or int64 into a varchar
// parameter ("cannot find encode plan") rather than coercing silently, so the
// conversion has to be explicit here. Changing the columns to a numeric type
// instead would break the existing reporting queries and the .NET writer during
// the parallel-run phase, so we match the schema as it is.
//
// FormatFloat with precision -1 emits the shortest representation that round
// trips (4.2 stays "4.2", not "4.2000000000000002").
func numStr(p *float64) any {
	if p == nil {
		return nil
	}
	return strconv.FormatFloat(*p, 'f', -1, 64)
}

func intStr(p *int64) any {
	if p == nil {
		return nil
	}
	return strconv.FormatInt(*p, 10)
}
