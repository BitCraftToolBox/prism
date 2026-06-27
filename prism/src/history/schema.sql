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

CREATE TABLE IF NOT EXISTS craft_history (
    entity_id                  BIGINT      PRIMARY KEY,
    owner_entity_id            BIGINT      NOT NULL,
    claim_entity_id            BIGINT      NOT NULL,
    building_entity_id         BIGINT      NOT NULL,
    first_seen                 TIMESTAMPTZ NOT NULL,
    recipe_id                  INTEGER     NOT NULL,
    count                      INTEGER     NOT NULL,
    region_id                  SMALLINT    NOT NULL,
    public                     BOOLEAN     NOT NULL,
    progress                   INTEGER     NOT NULL,
    last_seen                  TIMESTAMPTZ NOT NULL,
    recipe_effort_required     INTEGER,
    recipe_skill_id            INTEGER,
    recipe_exp_per_progress    REAL,
    recipe_level_required      INTEGER
);

CREATE INDEX IF NOT EXISTS craft_history_last_seen_idx
    ON craft_history (last_seen DESC);

CREATE INDEX IF NOT EXISTS craft_history_recipe_idx
    ON craft_history (recipe_id);

CREATE TABLE IF NOT EXISTS craft_contribution_history (
    craft_id      BIGINT  NOT NULL REFERENCES craft_history(entity_id),
    player_id     BIGINT  NOT NULL,
    contribution  INTEGER NOT NULL,
    PRIMARY KEY (craft_id, player_id)
);

CREATE INDEX IF NOT EXISTS craft_contribution_player_idx
    ON craft_contribution_history (player_id);
