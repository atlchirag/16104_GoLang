-- Minimal, production-compatible schema for LOCAL TESTING of the thin slice.
-- Column names match the real atltracking tables so the consumer's INSERT is
-- the same statement it will run in production. Run this once against your test
-- database:  psql -h localhost -U postgres -d atltracking -f schema.sql

-- ---------------------------------------------------------------------------
-- Device / service mapping. The consumer resolves IMEI -> service_id via this
-- join (same query as General.GetServiceID). Seed one row for the captured
-- test device so the pipeline has something to resolve.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tbl_devices (
    id    bigserial PRIMARY KEY,
    imei  text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS tbl_services (
    id            bigserial PRIMARY KEY,
    sys_device_id bigint NOT NULL REFERENCES tbl_devices(id),
    port_no       int
);

INSERT INTO tbl_devices (imei) VALUES ('864180055638907')
    ON CONFLICT (imei) DO NOTHING;

INSERT INTO tbl_services (sys_device_id, port_no)
    SELECT d.id, 16104 FROM tbl_devices d WHERE d.imei = '864180055638907'
    ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Daily telemetry table. The captured packet's gps_time is 2026-05-27, which
-- (with the +330 India offset) lands in tbl_telemetry_27052026.
--
-- In production these tables are created the night before by the
-- daily-telemetry service (atlchirag/Daily_Telemetry), which emits the
-- full 56-column schema plus the tbl_latest_telemetry trigger. What follows is
-- only the cut-down subset local tests need; do not treat it as the production
-- shape. To make one for a different day locally:
--
--   daily-telemetry -dry-run -date 2026-05-27 | psql -d atltracking
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tbl_telemetry_27052026 (
    id                    bigserial PRIMARY KEY,
    sys_service_id        bigint       NOT NULL,
    sys_proc_time         timestamptz  NOT NULL,
    gps_time              timestamptz  NOT NULL,
    gps_validity          text,
    gps_latitude          double precision,
    latitude_direction    text,
    gps_longitude         double precision,
    longitude_direction   text,
    gps_speed             double precision,
    gps_orientation       double precision,
    variation             double precision,
    checksum              text,
    i1  int, i2  int, i3  int, i4  int, i5  int, i6  int, i7  int, i8  int,
    i9  int, i10 int, i11 int, i12 int, i13 int, i14 int, i15 int,
    tel_odometer          double precision,
    battery_voltage       double precision,
    signal_strength       bigint,
    mobile_country_code   text,
    mobile_network_code   text,
    location_area_code    text,
    cell_id               text,
    firmware_version      text,
    raw_data              jsonb
);

-- Idempotency: at-least-once delivery means the same packet can arrive twice.
-- This unique key makes ON CONFLICT DO NOTHING a no-op on redelivery.
CREATE UNIQUE INDEX IF NOT EXISTS ux_tbl_telemetry_27052026_service_gpstime
    ON tbl_telemetry_27052026 (sys_service_id, gps_time);
