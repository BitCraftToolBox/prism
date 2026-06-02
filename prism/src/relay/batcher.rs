//! Latency-tiered batcher for the relay sink.
//!
//! Three flush timers: players ~250ms, enemies ~500ms, resources ~1000ms.
//! Replace* messages flush immediately (in chunks to stay under the 32MB
//! WebSocket limit). Deletes are batched with their respective pipeline.
//!
//! # Reconnect behaviour
//! Before every flush-tick (players, enemies, resources) the batcher calls
//! `ensure_connected`, which checks `conn.is_active()` and, if the connection
//! has gone away, waits `RECONNECT_DELAY` and reconnects. Any upsert/delete
//! rows buffered in `Batches` at the moment of disconnect are retained and
//! will be flushed once the new connection is up.
//! Replace* snapshot messages are re-queued to the processor by the upstream
//! task if needed — but in practice the downstream relay will simply receive
//! a fresh bulk snapshot on the next upstream reconnect cycle.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use relay_bindings::{EnemyLocation, PlayerLocation, ResourceLocation};
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, interval_at};
use log::{debug, error, info, warn};

use super::{EnemyRow, PlayerRow, RelayMsg, ResourceRow};
use super::connection::{RECONNECT_DELAY, RelayConnection};
use crate::config::Config;
use crate::shutdown::SharedShutdown;

const PLAYER_FLUSH_MS: u64 = 500;
const ENEMY_FLUSH_MS: u64 = 1000;
const RESOURCE_FLUSH_MS: u64 = 2500;

const MAX_BATCH: usize = 1024;

/// Max rows per reducer call for bulk Replace messages.
/// Each ResourceLocation row is ~21 bytes (BSATN); 500 000 rows ≈ 10 MB,
/// well under the 32 MB WebSocket message limit.
const BULK_REPLACE_CHUNK: usize = 500_000;

#[derive(Default)]
struct Batches {
    resource_upserts: Vec<ResourceLocation>,
    resource_deletes: Vec<u64>,
    enemy_upserts: Vec<EnemyLocation>,
    enemy_deletes: Vec<u64>,
    player_upserts: Vec<PlayerLocation>,
    player_deletes: Vec<u64>,
}

fn to_resource_location(r: &ResourceRow) -> ResourceLocation {
    ResourceLocation { entity_id: r.entity_id, resource_id: r.resource_id, region_id: r.region_id, x: r.x, z: r.z }
}
fn to_enemy_location(r: &EnemyRow) -> EnemyLocation {
    EnemyLocation { entity_id: r.entity_id, enemy_type: r.enemy_type, region_id: r.region_id, x: r.x, z: r.z }
}
fn to_player_location(r: &PlayerRow) -> PlayerLocation {
    PlayerLocation { entity_id: r.entity_id, region_id: r.region_id, x: r.x, z: r.z }
}

/// Attempt to connect, retrying with backoff until successful or shutdown.
/// Returns `None` if shutdown was triggered before a connection was made.
async fn connect_with_retry(config: &Config, shutdown: &SharedShutdown) -> Option<RelayConnection> {
    loop {
        match RelayConnection::connect(&config.relay).await {
            Ok(c) => return Some(c),
            Err(e) => {
                error!("relay batcher: connection failed: {e:?}; retrying in {RECONNECT_DELAY:?}");
                let sig = shutdown.lock().await.register()?;
                tokio::select! {
                    _ = sig => return None,
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                }
            }
        }
    }
}

/// If the connection is no longer active, reconnect in place.
/// Returns `false` if shutdown was triggered and the caller should exit.
async fn ensure_connected(
    conn: &mut RelayConnection,
    config: &Config,
    shutdown: &SharedShutdown,
) -> bool {
    if conn.is_active() {
        return true;
    }
    warn!("relay batcher: connection lost, reconnecting...");
    match connect_with_retry(config, shutdown).await {
        Some(new_conn) => {
            let old = std::mem::replace(conn, new_conn);
            old.disconnect();
            info!("relay batcher: reconnected");
            true
        }
        None => false,
    }
}

pub async fn run(
    config: Arc<Config>,
    mut rx: Receiver<RelayMsg>,
    shutdown: SharedShutdown,
) -> Result<()> {
    info!("relay batcher: starting");

    let Some(initial) = connect_with_retry(&config, &shutdown).await else {
        return Ok(());
    };
    let mut conn = initial;
    info!("relay batcher: connected, starting flush loops");

    let Some(shutdown_signal) = shutdown.lock().await.register() else {
        conn.disconnect();
        return Ok(());
    };
    tokio::pin!(shutdown_signal);

    let now = Instant::now();
    let mut player_tick   = interval_at(now + Duration::from_millis(PLAYER_FLUSH_MS),   Duration::from_millis(PLAYER_FLUSH_MS));
    let mut enemy_tick    = interval_at(now + Duration::from_millis(ENEMY_FLUSH_MS),    Duration::from_millis(ENEMY_FLUSH_MS));
    let mut resource_tick = interval_at(now + Duration::from_millis(RESOURCE_FLUSH_MS), Duration::from_millis(RESOURCE_FLUSH_MS));

    let mut batches = Batches::default();

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown_signal => {
                info!("relay batcher: shutdown signal received");
                break;
            }

            msg = rx.recv() => {
                let Some(msg) = msg else {
                    info!("relay batcher: upstream channel closed");
                    break;
                };
                match msg {
                    // Bulk replace: clear region then insert in chunks.
                    // First chunk uses bulk_replace_* (which deletes existing rows for
                    // the region first); subsequent chunks use upsert_* so we don't
                    // wipe what we just inserted.
                    RelayMsg::ReplaceResources { region_id, rows } => {
                        flush_resource_batch(&conn, &mut batches);
                        let relay_rows: Vec<ResourceLocation> = rows.iter().map(to_resource_location).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.bulk_replace_resources(region_id, chunk, rows.len() as u32),
                            |chunk| conn.upsert_resources(chunk),
                            "resources",
                        );
                    }
                    RelayMsg::ReplaceEnemies { region_id, rows } => {
                        flush_enemy_batch(&conn, &mut batches);
                        let relay_rows: Vec<EnemyLocation> = rows.iter().map(to_enemy_location).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.bulk_replace_enemies(region_id, chunk, rows.len() as u32),
                            |chunk| conn.upsert_enemies(chunk),
                            "enemies",
                        );
                    }
                    RelayMsg::ReplacePlayers { region_id, rows } => {
                        flush_player_batch(&conn, &mut batches);
                        let relay_rows: Vec<PlayerLocation> = rows.iter().map(to_player_location).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.bulk_replace_players(region_id, chunk, rows.len() as u32),
                            |chunk| conn.upsert_players(chunk),
                            "players",
                        );
                    }
                    RelayMsg::UpsertResource(row) => {
                        batches.resource_upserts.push(to_resource_location(&row));
                        if batches.resource_upserts.len() >= MAX_BATCH { flush_resource_batch(&conn, &mut batches); }
                    }
                    RelayMsg::UpsertEnemy(row) => {
                        batches.enemy_upserts.push(to_enemy_location(&row));
                        if batches.enemy_upserts.len() >= MAX_BATCH { flush_enemy_batch(&conn, &mut batches); }
                    }
                    RelayMsg::UpsertPlayer(row) => {
                        batches.player_upserts.push(to_player_location(&row));
                        if batches.player_upserts.len() >= MAX_BATCH { flush_player_batch(&conn, &mut batches); }
                    }
                    RelayMsg::DeleteResource(id) => {
                        batches.resource_deletes.push(id);
                        if batches.resource_deletes.len() >= MAX_BATCH { flush_resource_batch(&conn, &mut batches); }
                    }
                    RelayMsg::DeleteEnemy(id) => {
                        batches.enemy_deletes.push(id);
                        if batches.enemy_deletes.len() >= MAX_BATCH { flush_enemy_batch(&conn, &mut batches); }
                    }
                    RelayMsg::DeletePlayer(id) => {
                        batches.player_deletes.push(id);
                        if batches.player_deletes.len() >= MAX_BATCH { flush_player_batch(&conn, &mut batches); }
                    }
                }
            }

            _ = player_tick.tick() => {
                if !ensure_connected(&mut conn, &config, &shutdown).await { break; }
                flush_player_batch(&conn, &mut batches);
            }
            _ = enemy_tick.tick() => {
                if !ensure_connected(&mut conn, &config, &shutdown).await { break; }
                flush_enemy_batch(&conn, &mut batches);
            }
            _ = resource_tick.tick() => {
                if !ensure_connected(&mut conn, &config, &shutdown).await { break; }
                flush_resource_batch(&conn, &mut batches);
            }
        }
    }

    // Final flush, then cleanly disconnect.
    flush_resource_batch(&conn, &mut batches);
    flush_enemy_batch(&conn, &mut batches);
    flush_player_batch(&conn, &mut batches);
    info!("relay batcher: disconnecting...");
    conn.disconnect();
    info!("relay batcher: exited");
    Ok(())
}

/// Send a large set of rows as: chunk[0] via `replace_fn` (deletes region
/// first), chunks[1..] via `upsert_fn` (insert-only).
fn bulk_replace_chunked<T, FR, FU>(
    rows: Vec<T>,
    replace_fn: FR,
    upsert_fn: FU,
    kind: &'static str,
)
where
    T: Clone,
    FR: Fn(Vec<T>) -> Result<()>,
    FU: Fn(Vec<T>) -> Result<()>,
{
    if rows.is_empty() { return; }
    let chunks: Vec<&[T]> = rows.chunks(BULK_REPLACE_CHUNK).collect();
    info!("relay: bulk_replace_{kind} count={} chunks={}", rows.len(), chunks.len());
    for (i, chunk) in chunks.iter().enumerate() {
        let result = if i == 0 {
            replace_fn(chunk.to_vec())
        } else {
            upsert_fn(chunk.to_vec())
        };
        if let Err(e) = result {
            warn!("relay: bulk_{kind} chunk {i} failed: {e:?}");
        }
    }
}

fn flush_resource_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.resource_upserts.is_empty() {
        let rows = std::mem::take(&mut batches.resource_upserts);
        debug!("relay flush: upsert_resources count={}", rows.len());
        if let Err(e) = conn.upsert_resources(rows) { warn!("relay: upsert_resources: {e:?}"); }
    }
    if !batches.resource_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.resource_deletes);
        debug!("relay flush: delete_resources count={}", ids.len());
        if let Err(e) = conn.delete_resources(ids) { warn!("relay: delete_resources: {e:?}"); }
    }
}

fn flush_enemy_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.enemy_upserts.is_empty() {
        let rows = std::mem::take(&mut batches.enemy_upserts);
        debug!("relay flush: upsert_enemies count={}", rows.len());
        if let Err(e) = conn.upsert_enemies(rows) { warn!("relay: upsert_enemies: {e:?}"); }
    }
    if !batches.enemy_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.enemy_deletes);
        debug!("relay flush: delete_enemies count={}", ids.len());
        if let Err(e) = conn.delete_enemies(ids) { warn!("relay: delete_enemies: {e:?}"); }
    }
}

fn flush_player_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.player_upserts.is_empty() {
        let rows = std::mem::take(&mut batches.player_upserts);
        debug!("relay flush: upsert_players count={}", rows.len());
        if let Err(e) = conn.upsert_players(rows) { warn!("relay: upsert_players: {e:?}"); }
    }
    if !batches.player_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.player_deletes);
        debug!("relay flush: delete_players count={}", ids.len());
        if let Err(e) = conn.delete_players(ids) { warn!("relay: delete_players: {e:?}"); }
    }
}
