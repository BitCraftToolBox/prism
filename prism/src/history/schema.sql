-- TimescaleDB schema for Prism's historical data.
-- Applied via sqlx::raw_sql on startup (idempotent).

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS player_locations (
    entity_id   BIGINT      NOT NULL,
    x           INTEGER     NOT NULL,
    z           INTEGER     NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('player_locations', 'recorded_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE);

SELECT add_retention_policy('player_locations', INTERVAL '14 days', true);

CREATE INDEX IF NOT EXISTS player_locations_entity_idx
    ON player_locations (entity_id, recorded_at DESC);
