//! History sink — append-only TimescaleDB writer.
//!
//! Batched insert with per-entity dedup of consecutive identical positions.
//! Uses `sqlx::PgPool` and flushes every N rows or T ms via unnest bulk insert.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use hashbrown::HashMap;
use log::{debug, error, info, warn};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, interval_at};

use crate::config::Config;
use crate::shutdown::SharedShutdown;

pub fn history_capacity(_config: &Config) -> usize {
    16384
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
}

pub async fn run(
    config: Arc<Config>,
    mut rx: Receiver<HistoryMsg>,
    shutdown: SharedShutdown,
) -> Result<()> {
    if config.database.url.is_empty() {
        info!("Database not configured, history will not be stored.");
        return Ok(());
    }
    info!("history: connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&config.database.url)
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
    let mut last_pos: HashMap<u64, (i32, i32)> = HashMap::new();

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
                        // Dedup: skip if same large hex tile as last sample.
                        let cell = (x / 3000, z / 3000);
                        if last_pos.get(&entity_id).is_some_and(|prev| *prev == cell) {
                            continue;
                        }
                        last_pos.insert(entity_id, cell);
                        buffer.push(PlayerLocationRow { entity_id, timestamp, x, z });
                        if buffer.len() >= MAX_BATCH {
                            flush(&pool, &mut buffer).await;
                        }
                    }
                }
            }

            _ = flush_tick.tick() => {
                if !buffer.is_empty() {
                    flush(&pool, &mut buffer).await;
                }
            }
        }
    }

    if !buffer.is_empty() {
        flush(&pool, &mut buffer).await;
    }
    Ok(())
}

struct PlayerLocationRow {
    entity_id: u64,
    timestamp: u64,
    x: i32,
    z: i32,
}

async fn flush(pool: &PgPool, buffer: &mut Vec<PlayerLocationRow>) {
    let rows = std::mem::take(buffer);
    if rows.is_empty() {
        return;
    }
    debug!(
        "history flush: inserting player_locations count={}",
        rows.len()
    );

    // Collect column arrays for unnest bulk insert.
    // Casting u64 → i64 is safe: game entity IDs and timestamps (µs since epoch)
    // are well within the signed i64 range for any reasonable game timestamp.
    let entity_ids: Vec<i64> = rows.iter().map(|r| r.entity_id as i64).collect();
    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp as i64).collect();
    let xs: Vec<i32> = rows.iter().map(|r| r.x).collect();
    let zs: Vec<i32> = rows.iter().map(|r| r.z).collect();

    if let Err(e) = sqlx::query(
        "INSERT INTO player_locations (entity_id, x, z, recorded_at) \
         SELECT entity_id, x, z, \
                'epoch'::timestamptz + ts_us * interval '1 microsecond' \
         FROM unnest($1::bigint[], $2::int[], $3::int[], $4::bigint[]) \
              AS t(entity_id, x, z, ts_us)",
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
}
