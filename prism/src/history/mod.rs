//! History sink — writes historical data to PostgreSQL/TimescaleDB.
//!
//! Player locations are append-only samples. Craft history uses upserts and
//! partial updates so progress/public/contribution changes can be applied
//! without resending the full craft row.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use hashbrown::HashMap;
use log::{debug, error, info, warn};
use metrics::{counter, histogram};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, interval_at};

use crate::config::Config;
use crate::relay::{
    CraftContributionDeltaRow, CraftPublicUpdateRow, CraftUpdateRow, RecipeMetaRow,
};
use crate::shutdown::SharedShutdown;

pub fn history_capacity(_config: &Config) -> usize {
    16384
}

pub fn history_enabled(config: &Config) -> bool {
    config
        .database
        .url
        .as_ref()
        .is_some_and(|url| !url.is_empty())
}

const FLUSH_INTERVAL_MS: u64 = 5000;
const MAX_BATCH: usize = 500;

#[derive(Debug, Clone)]
pub enum HistoryMsg {
    PlayerLocation {
        entity_id: u64,
        timestamp: u64,
        x: i32,
        z: i32,
    },
    UpsertRecipeMeta(Vec<RecipeMetaRow>),
    DeleteRecipeMeta(Vec<i32>),
    UpsertCrafts(Vec<CraftUpdateRow>),
    ToggleCraftPublic(Vec<CraftPublicUpdateRow>),
    ApplyCraftProgressDeltas(Vec<CraftContributionDeltaRow>),
}

pub async fn run(
    config: Arc<Config>,
    rx: Option<Receiver<HistoryMsg>>,
    shutdown: SharedShutdown,
) -> Result<()> {
    let Some(mut rx) = rx else {
        info!("Database not configured, history will not be stored.");
        return Ok(());
    };
    // history_enabled was checked in channels(); url is guaranteed non-empty here.
    let url = config
        .database
        .url
        .as_ref()
        .expect("history enabled but no database url");
    info!("history: connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.as_str())
        .await?;
    info!("history: connected, running migrations");

    sqlx::raw_sql(include_str!("schema.sql"))
        .execute(&pool)
        .await
        .map_err(|e| {
            error!("history: migration failed: {e:?}");
            e
        })?;
    info!("history: migrations applied, starting writer");

    let Some(shutdown_signal) = shutdown.lock().await.register() else {
        return Ok(());
    };
    tokio::pin!(shutdown_signal);

    let mut buffer: Vec<PlayerLocationRow> = Vec::new();
    let mut recipe_meta: HashMap<i32, RecipeMetaSnapshot> = HashMap::new();

    let now = Instant::now();
    let mut flush_tick = interval_at(
        now + Duration::from_millis(FLUSH_INTERVAL_MS),
        Duration::from_millis(FLUSH_INTERVAL_MS),
    );

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown_signal => {
                info!("history: shutdown signal received");
                break;
            }

            msg = rx.recv() => {
                let Some(msg) = msg else {
                    info!("history: upstream channel closed");
                    break;
                };
                match msg {
                    HistoryMsg::PlayerLocation { entity_id, timestamp, x, z } => {
                        buffer.push(PlayerLocationRow { entity_id, timestamp, x, z });
                        if buffer.len() >= MAX_BATCH {
                            flush_player_locations(&pool, &mut buffer).await;
                        }
                    }
                    HistoryMsg::UpsertRecipeMeta(rows) => {
                        for row in rows {
                            recipe_meta.insert(
                                row.id,
                                RecipeMetaSnapshot {
                                    effort_required: row.effort_required,
                                    skill_id: row.skill_id,
                                    exp_per_progress: row.exp_per_progress,
                                    level_required: row.level_required,
                                },
                            );
                        }
                    }
                    HistoryMsg::DeleteRecipeMeta(ids) => {
                        for id in ids {
                            recipe_meta.remove(&id);
                        }
                    }
                    HistoryMsg::UpsertCrafts(rows) => {
                        upsert_crafts(&pool, &rows, &recipe_meta).await;
                    }
                    HistoryMsg::ToggleCraftPublic(rows) => {
                        update_craft_public(&pool, &rows).await;
                    }
                    HistoryMsg::ApplyCraftProgressDeltas(rows) => {
                        apply_craft_progress_deltas(&pool, &rows).await;
                    }
                }
            }

            _ = flush_tick.tick() => {
                if !buffer.is_empty() {
                    flush_player_locations(&pool, &mut buffer).await;
                }
            }
        }
    }

    if !buffer.is_empty() {
        flush_player_locations(&pool, &mut buffer).await;
    }
    Ok(())
}

struct PlayerLocationRow {
    entity_id: u64,
    timestamp: u64,
    x: i32,
    z: i32,
}

#[derive(Clone, Copy)]
struct RecipeMetaSnapshot {
    effort_required: i32,
    skill_id: i32,
    exp_per_progress: f32,
    level_required: i32,
}

async fn flush_player_locations(pool: &PgPool, buffer: &mut Vec<PlayerLocationRow>) {
    let rows = std::mem::take(buffer);
    if rows.is_empty() {
        return;
    }
    debug!(
        "history flush: inserting player_locations count={}",
        rows.len()
    );
    counter!("prism_history_rows_total", "op" => "player_locations").increment(rows.len() as u64);
    let t = std::time::Instant::now();

    // Collect column arrays for unnest bulk insert.
    // Casting u64 → i64 is safe: game entity IDs and timestamps (ms since epoch)
    // are well within the signed i64 range for any reasonable game timestamp.
    let entity_ids: Vec<i64> = rows.iter().map(|r| r.entity_id as i64).collect();
    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp as i64).collect();
    let xs: Vec<i32> = rows.iter().map(|r| r.x).collect();
    let zs: Vec<i32> = rows.iter().map(|r| r.z).collect();

    if let Err(e) = sqlx::query(
        "INSERT INTO player_locations (entity_id, x, z, recorded_at) \
         SELECT entity_id, x, z, to_timestamp(ts_ms::double precision / 1000.0) \
         FROM unnest($1::bigint[], $2::int[], $3::int[], $4::bigint[]) \
              AS t(entity_id, x, z, ts_ms)",
    )
    .bind(&entity_ids[..])
    .bind(&xs[..])
    .bind(&zs[..])
    .bind(&timestamps[..])
    .execute(pool)
    .await
    {
        warn!("history: batch insert failed: {e:?}");
    }
    histogram!("prism_history_flush_duration_seconds", "op" => "player_locations")
        .record(t.elapsed().as_secs_f64());
}

async fn upsert_crafts(
    pool: &PgPool,
    rows: &[CraftUpdateRow],
    recipe_meta: &HashMap<i32, RecipeMetaSnapshot>,
) {
    if rows.is_empty() {
        return;
    }
    debug!("history flush: upserting crafts count={}", rows.len());
    counter!("prism_history_rows_total", "op" => "craft_upsert").increment(rows.len() as u64);
    let t = std::time::Instant::now();

    let entity_ids: Vec<i64> = rows.iter().map(|r| r.entity_id as i64).collect();
    let owner_ids: Vec<i64> = rows.iter().map(|r| r.owner_entity_id as i64).collect();
    let claim_ids: Vec<i64> = rows.iter().map(|r| r.claim_entity_id as i64).collect();
    let building_ids: Vec<i64> = rows.iter().map(|r| r.building_entity_id as i64).collect();
    let first_seen_micros: Vec<i64> = rows.iter().map(|r| r.first_seen_micros).collect();
    let recipe_ids: Vec<i32> = rows.iter().map(|r| r.recipe_id).collect();
    let craft_counts: Vec<i32> = rows.iter().map(|r| r.count).collect();
    let region_ids: Vec<i16> = rows.iter().map(|r| r.region_id as i16).collect();
    let public_flags: Vec<bool> = rows.iter().map(|r| r.public).collect();
    let progress_values: Vec<i32> = rows.iter().map(|r| r.progress).collect();
    let last_seen_micros: Vec<i64> = rows.iter().map(|r| r.last_seen_micros).collect();
    let recipe_effort_required: Vec<Option<i32>> = rows
        .iter()
        .map(|r| recipe_meta.get(&r.recipe_id).map(|m| m.effort_required))
        .collect();
    let recipe_skill_id: Vec<Option<i32>> = rows
        .iter()
        .map(|r| recipe_meta.get(&r.recipe_id).map(|m| m.skill_id))
        .collect();
    let recipe_exp_per_progress: Vec<Option<f32>> = rows
        .iter()
        .map(|r| recipe_meta.get(&r.recipe_id).map(|m| m.exp_per_progress))
        .collect();
    let recipe_level_required: Vec<Option<i32>> = rows
        .iter()
        .map(|r| recipe_meta.get(&r.recipe_id).map(|m| m.level_required))
        .collect();

    if let Err(e) = sqlx::query(
        "INSERT INTO craft_history (
            entity_id,
            owner_entity_id,
            claim_entity_id,
            building_entity_id,
            first_seen,
            recipe_id,
            count,
            region_id,
            public,
            progress,
            last_seen,
            recipe_effort_required,
            recipe_skill_id,
            recipe_exp_per_progress,
            recipe_level_required
        )
        SELECT
            t.entity_id,
            t.owner_entity_id,
            t.claim_entity_id,
            t.building_entity_id,
            to_timestamp(t.first_seen_micros::double precision / 1000000.0),
            t.recipe_id,
            t.craft_count,
            t.region_id,
            t.public,
            t.progress,
            to_timestamp(t.last_seen_micros::double precision / 1000000.0),
            t.recipe_effort_required,
            t.recipe_skill_id,
            t.recipe_exp_per_progress,
            t.recipe_level_required
        FROM unnest(
            $1::bigint[],
            $2::bigint[],
            $3::bigint[],
            $4::bigint[],
            $5::bigint[],
            $6::int[],
            $7::int[],
            $8::smallint[],
            $9::bool[],
            $10::int[],
            $11::bigint[],
            $12::int[],
            $13::int[],
            $14::real[],
            $15::int[]
        ) AS t(
            entity_id,
            owner_entity_id,
            claim_entity_id,
            building_entity_id,
            first_seen_micros,
            recipe_id,
            craft_count,
            region_id,
            public,
            progress,
            last_seen_micros,
            recipe_effort_required,
            recipe_skill_id,
            recipe_exp_per_progress,
            recipe_level_required
        )
        ON CONFLICT (entity_id) DO UPDATE
        SET
            owner_entity_id = EXCLUDED.owner_entity_id,
            claim_entity_id = EXCLUDED.claim_entity_id,
            building_entity_id = EXCLUDED.building_entity_id,
            first_seen = craft_history.first_seen,
            recipe_id = EXCLUDED.recipe_id,
            count = EXCLUDED.count,
            region_id = EXCLUDED.region_id,
            public = EXCLUDED.public,
            progress = EXCLUDED.progress,
            last_seen = EXCLUDED.last_seen,
            recipe_effort_required = craft_history.recipe_effort_required,
            recipe_skill_id = craft_history.recipe_skill_id,
            recipe_exp_per_progress = craft_history.recipe_exp_per_progress,
            recipe_level_required = craft_history.recipe_level_required",
    )
    .bind(&entity_ids[..])
    .bind(&owner_ids[..])
    .bind(&claim_ids[..])
    .bind(&building_ids[..])
    .bind(&first_seen_micros[..])
    .bind(&recipe_ids[..])
    .bind(&craft_counts[..])
    .bind(&region_ids[..])
    .bind(&public_flags[..])
    .bind(&progress_values[..])
    .bind(&last_seen_micros[..])
    .bind(&recipe_effort_required[..])
    .bind(&recipe_skill_id[..])
    .bind(&recipe_exp_per_progress[..])
    .bind(&recipe_level_required[..])
    .execute(pool)
    .await
    {
        warn!("history: craft upsert failed: {e:?}");
    }
    histogram!("prism_history_flush_duration_seconds", "op" => "craft_upsert")
        .record(t.elapsed().as_secs_f64());
}

async fn update_craft_public(pool: &PgPool, rows: &[CraftPublicUpdateRow]) {
    if rows.is_empty() {
        return;
    }
    let craft_ids: Vec<i64> = rows.iter().map(|r| r.craft_id as i64).collect();
    let public_flags: Vec<bool> = rows.iter().map(|r| r.public).collect();

    if let Err(e) = sqlx::query(
        "UPDATE craft_history c
         SET public = src.public
         FROM unnest($1::bigint[], $2::bool[]) AS src(craft_id, public)
         WHERE c.entity_id = src.craft_id",
    )
    .bind(&craft_ids[..])
    .bind(&public_flags[..])
    .execute(pool)
    .await
    {
        warn!("history: craft public update failed: {e:?}");
    }
}

async fn apply_craft_progress_deltas(pool: &PgPool, rows: &[CraftContributionDeltaRow]) {
    if rows.is_empty() {
        return;
    }
    let rows: Vec<&CraftContributionDeltaRow> =
        rows.iter().filter(|r| r.progress_delta != 0).collect();
    if rows.is_empty() {
        return;
    }
    counter!("prism_history_rows_total", "op" => "craft_progress").increment(rows.len() as u64);
    let t = std::time::Instant::now();

    let craft_ids: Vec<i64> = rows.iter().map(|r| r.craft_id as i64).collect();
    let player_ids: Vec<i64> = rows.iter().map(|r| r.player_id as i64).collect();
    let deltas: Vec<i32> = rows.iter().map(|r| r.progress_delta).collect();
    let progress_totals: Vec<i32> = rows.iter().map(|r| r.progress_total).collect();
    let last_seen_micros: Vec<i64> = rows.iter().map(|r| r.last_seen_micros).collect();

    if let Err(e) = sqlx::query(
        "INSERT INTO craft_contribution_history (craft_id, player_id, contribution)
         SELECT src.craft_id, src.player_id, src.progress_delta
         FROM unnest($1::bigint[], $2::bigint[], $3::int[]) AS src(craft_id, player_id, progress_delta)
         INNER JOIN craft_history c ON c.entity_id = src.craft_id
         ON CONFLICT (craft_id, player_id) DO UPDATE
         SET contribution = craft_contribution_history.contribution + EXCLUDED.contribution",
    )
    .bind(&craft_ids[..])
    .bind(&player_ids[..])
    .bind(&deltas[..])
    .execute(pool)
    .await
    {
        warn!("history: craft contribution upsert failed: {e:?}");
    }

    if let Err(e) = sqlx::query(
        "UPDATE craft_history c
         SET
            progress = src.progress_total,
            last_seen = to_timestamp(src.last_seen_micros::double precision / 1000000.0)
         FROM unnest($1::bigint[], $2::int[], $3::bigint[]) AS src(craft_id, progress_total, last_seen_micros)
         WHERE c.entity_id = src.craft_id",
    )
    .bind(&craft_ids[..])
    .bind(&progress_totals[..])
    .bind(&last_seen_micros[..])
    .execute(pool)
    .await
    {
        warn!("history: craft progress update failed: {e:?}");
    }
    histogram!("prism_history_flush_duration_seconds", "op" => "craft_progress")
        .record(t.elapsed().as_secs_f64());
}
