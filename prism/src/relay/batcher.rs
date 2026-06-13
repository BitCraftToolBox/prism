//! Latency-tiered batcher for the relay sink.
//!
//! Three flush timers: players ~500ms, enemies ~1000ms, resources ~2500ms.
//! Replace* messages flush immediately (in chunks to stay under the 32MB
//! WebSocket limit). Deletes are batched with their respective pipeline.
//!
//! # Reconnect behavior
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
use log::{debug, error, info, warn};
use relay_bindings::{
    EnemyLocation, GrowthTimerUpdate, MobileMoveUpdate, PlayerLocation, PlayerRenameUpdate,
    PlayerState, ResourceLocation,
};
use relay_sdk::Timestamp;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, interval_at};

use super::connection::{RECONNECT_DELAY, RelayConnection};
use super::{
    EnemyRow, GrowthTimerRow, MobileMoveRow, PlayerRenameRow, PlayerRow, PlayerStateRow, RelayMsg,
    ResourceRow,
};
use crate::config::{Config, RelayConfig};
use crate::shutdown::SharedShutdown;

const PLAYER_FLUSH_MS: u64 = 500;
const ENEMY_FLUSH_MS: u64 = 1000;
const RESOURCE_FLUSH_MS: u64 = 2500;

const MAX_BATCH: usize = 50_000;

/// Max rows per reducer call for bulk Replace messages.
/// Each ResourceLocation row is ~21 bytes (BSATN); 500 000 rows ≈ 10 MB,
/// well under the 32 MB WebSocket message limit.
const BULK_REPLACE_CHUNK: usize = 500_000;

#[derive(Default)]
struct Batches {
    resource_inserts: Vec<ResourceLocation>,
    resource_deletes: Vec<u64>,
    growth_timer_inserts: Vec<GrowthTimerUpdate>,
    enemy_inserts: Vec<EnemyLocation>,
    enemy_deletes: Vec<u64>,
    player_upserts: Vec<PlayerLocation>,
    player_deletes: Vec<u64>,
    player_state_upserts: Vec<PlayerState>,
    player_state_deletes: Vec<u64>,
    /// Live-phase: location updates for existing entities (relay resolves type).
    mobile_moves: Vec<MobileMoveUpdate>,
    /// Live-phase: entity_ids to mark online.
    player_online_ids: Vec<u64>,
    /// Live-phase: entity_ids to mark offline.
    player_offline_ids: Vec<u64>,
    /// Live-phase: name-only updates for existing player_state rows.
    player_renames: Vec<PlayerRenameUpdate>,
}

fn to_resource_location(r: &ResourceRow) -> ResourceLocation {
    ResourceLocation {
        entity_id: r.entity_id,
        resource_id: r.resource_id,
        region_id: r.region_id,
        x: r.x,
        z: r.z,
    }
}
fn to_enemy_location(r: &EnemyRow) -> EnemyLocation {
    EnemyLocation {
        entity_id: r.entity_id,
        enemy_type: r.enemy_type,
        region_id: r.region_id,
        x: r.x,
        z: r.z,
    }
}
fn to_growth_timer_update(r: &GrowthTimerRow) -> GrowthTimerUpdate {
    GrowthTimerUpdate {
        entity_id: r.entity_id,
        end_timestamp: Timestamp::from_micros_since_unix_epoch(r.end_timestamp_micros),
    }
}
fn to_player_location(r: &PlayerRow) -> PlayerLocation {
    PlayerLocation {
        entity_id: r.entity_id,
        region_id: r.region_id,
        x: r.x,
        z: r.z,
    }
}
fn to_player_state(r: &PlayerStateRow) -> PlayerState {
    PlayerState {
        entity_id: r.entity_id,
        region_id: r.region_id,
        online: r.online,
        name: r.name.clone(),
    }
}
fn to_mobile_move_update(r: &MobileMoveRow) -> MobileMoveUpdate {
    MobileMoveUpdate {
        entity_id: r.entity_id,
        region_id: r.region_id,
        x: r.x,
        z: r.z,
    }
}
fn to_player_rename_update(r: &PlayerRenameRow) -> PlayerRenameUpdate {
    PlayerRenameUpdate {
        entity_id: r.entity_id,
        name: r.name.clone(),
    }
}

/// Attempt to connect, retrying with backoff until successful or shutdown.
/// Returns `None` if shutdown was triggered before a connection was made.
async fn connect_with_retry(
    relay: &RelayConfig,
    shutdown: &SharedShutdown,
) -> Option<RelayConnection> {
    loop {
        match RelayConnection::connect(relay).await {
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
    relay: &RelayConfig,
    shutdown: &SharedShutdown,
) -> bool {
    if conn.is_active() {
        return true;
    }
    warn!("relay batcher: connection lost, reconnecting...");
    match connect_with_retry(relay, shutdown).await {
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

    // Safety: validated at config-load time — relay config is always present
    // when any pipeline is enabled, and run() is only called in that case.
    let relay = config
        .relay
        .as_ref()
        .expect("relay config required when pipelines are enabled");

    let Some(initial) = connect_with_retry(relay, &shutdown).await else {
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
    let mut player_tick = interval_at(
        now + Duration::from_millis(PLAYER_FLUSH_MS),
        Duration::from_millis(PLAYER_FLUSH_MS),
    );
    let mut enemy_tick = interval_at(
        now + Duration::from_millis(ENEMY_FLUSH_MS),
        Duration::from_millis(ENEMY_FLUSH_MS),
    );
    let mut resource_tick = interval_at(
        now + Duration::from_millis(RESOURCE_FLUSH_MS),
        Duration::from_millis(RESOURCE_FLUSH_MS),
    );

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
                    // the region first); subsequent chunks use insert_* so we don't
                    // wipe what we just inserted.
                    RelayMsg::ReplaceResources { region_id, rows } => {
                        flush_resource_batch(&conn, &mut batches);
                        flush_growth_batch(&conn, &mut batches);
                        let relay_rows: Vec<ResourceLocation> = rows.iter().map(to_resource_location).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.bulk_replace_resources(region_id, chunk, rows.len() as u32),
                            |chunk| conn.insert_resources(chunk),
                            "resources",
                            region_id,
                        );
                    }
                    RelayMsg::ReplaceGrowthTimers { region_id, rows } => {
                        // Growth timers depend on resource rows existing module-side.
                        flush_resource_batch(&conn, &mut batches);
                        flush_growth_batch(&conn, &mut batches);
                        let relay_rows: Vec<GrowthTimerUpdate> = rows.iter().map(to_growth_timer_update).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.insert_growth_timers(chunk),
                            |chunk| conn.insert_growth_timers(chunk),
                            "growth_timers",
                            region_id,
                        );
                    }
                    RelayMsg::ReplaceEnemies { region_id, rows } => {
                        flush_enemy_batch(&conn, &mut batches);
                        let relay_rows: Vec<EnemyLocation> = rows.iter().map(to_enemy_location).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.bulk_replace_enemies(region_id, chunk, rows.len() as u32),
                            |chunk| conn.insert_enemies(chunk),
                            "enemies",
                            region_id,
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
                            region_id,
                        );
                    }
                    RelayMsg::ReplacePlayerStates { region_id, rows } => {
                        flush_player_state_batch(&conn, &mut batches);
                        let relay_rows: Vec<PlayerState> = rows.iter().map(to_player_state).collect();
                        bulk_replace_chunked(
                            relay_rows,
                            |chunk| conn.bulk_replace_player_states(region_id, chunk, rows.len() as u32),
                            |chunk| conn.upsert_player_states(chunk),
                            "player_states",
                            region_id,
                        );
                    }
                    RelayMsg::InsertResource(row) => {
                        batches.resource_inserts.push(to_resource_location(&row));
                        if batches.resource_inserts.len() >= MAX_BATCH { flush_resource_batch(&conn, &mut batches); }
                    }
                    RelayMsg::InsertGrowthTimer(row) => {
                        batches.growth_timer_inserts.push(to_growth_timer_update(&row));
                        if batches.growth_timer_inserts.len() >= MAX_BATCH {
                            flush_resource_batch(&conn, &mut batches);
                            flush_growth_batch(&conn, &mut batches);
                        }
                    }
                    RelayMsg::InsertEnemy(row) => {
                        batches.enemy_inserts.push(to_enemy_location(&row));
                        if batches.enemy_inserts.len() >= MAX_BATCH { flush_enemy_batch(&conn, &mut batches); }
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
                    RelayMsg::UpsertPlayerState(row) => {
                        batches.player_state_upserts.push(to_player_state(&row));
                        if batches.player_state_upserts.len() >= MAX_BATCH { flush_player_state_batch(&conn, &mut batches); }
                    }
                    RelayMsg::MoveMobileEntities(moves) => {
                        batches.mobile_moves.extend(moves.iter().map(to_mobile_move_update));
                        if batches.mobile_moves.len() >= MAX_BATCH { flush_mobile_moves(&conn, &mut batches); }
                    }
                    RelayMsg::SetPlayersOnline(ids) => {
                        batches.player_online_ids.extend(ids);
                        if batches.player_online_ids.len() >= MAX_BATCH { flush_player_online(&conn, &mut batches); }
                    }
                    RelayMsg::SetPlayersOffline(ids) => {
                        batches.player_offline_ids.extend(ids);
                        if batches.player_offline_ids.len() >= MAX_BATCH { flush_player_offline(&conn, &mut batches); }
                    }
                    RelayMsg::RenamePlayers(renames) => {
                        batches.player_renames.extend(renames.iter().map(to_player_rename_update));
                        if batches.player_renames.len() >= MAX_BATCH { flush_player_renames(&conn, &mut batches); }
                    }
                }
            }

            _ = player_tick.tick() => {
                if !ensure_connected(&mut conn, relay, &shutdown).await { break; }
                flush_player_batch(&conn, &mut batches);
                flush_player_state_batch(&conn, &mut batches);
                flush_mobile_moves(&conn, &mut batches);
                flush_player_online(&conn, &mut batches);
                flush_player_offline(&conn, &mut batches);
                flush_player_renames(&conn, &mut batches);
            }
            _ = enemy_tick.tick() => {
                if !ensure_connected(&mut conn, relay, &shutdown).await { break; }
                flush_enemy_batch(&conn, &mut batches);
            }
            _ = resource_tick.tick() => {
                if !ensure_connected(&mut conn, relay, &shutdown).await { break; }
                flush_resource_batch(&conn, &mut batches);
                flush_growth_batch(&conn, &mut batches);
            }
        }
    }

    // Final flush, then cleanly disconnect.
    flush_resource_batch(&conn, &mut batches);
    flush_growth_batch(&conn, &mut batches);
    flush_enemy_batch(&conn, &mut batches);
    flush_player_batch(&conn, &mut batches);
    flush_player_state_batch(&conn, &mut batches);
    flush_mobile_moves(&conn, &mut batches);
    flush_player_online(&conn, &mut batches);
    flush_player_offline(&conn, &mut batches);
    flush_player_renames(&conn, &mut batches);
    info!("relay batcher: disconnecting...");
    conn.disconnect();
    Ok(())
}

/// Send a large set of rows as: chunk[0] via `replace_fn` (deletes region
/// first), chunks[1..] via `upsert_fn` (insert-only).
fn bulk_replace_chunked<T, FR, FU>(
    rows: Vec<T>,
    replace_fn: FR,
    upsert_fn: FU,
    kind: &'static str,
    region: u8,
) where
    T: Clone,
    FR: Fn(Vec<T>) -> Result<()>,
    FU: Fn(Vec<T>) -> Result<()>,
{
    if rows.is_empty() {
        return;
    }
    let chunks: Vec<&[T]> = rows.chunks(BULK_REPLACE_CHUNK).collect();
    info!(
        "relay: {} bulk_replace_{kind} count={} chunks={}",
        region,
        rows.len(),
        chunks.len()
    );
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
    if !batches.resource_inserts.is_empty() {
        let rows = std::mem::take(&mut batches.resource_inserts);
        debug!("relay flush: insert_resources count={}", rows.len());
        if let Err(e) = conn.insert_resources(rows) {
            warn!("relay: insert_resources: {e:?}");
        }
    }
    if !batches.resource_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.resource_deletes);
        debug!("relay flush: delete_resources count={}", ids.len());
        if let Err(e) = conn.delete_resources(ids) {
            warn!("relay: delete_resources: {e:?}");
        }
    }
}

fn flush_growth_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.growth_timer_inserts.is_empty() {
        let rows = std::mem::take(&mut batches.growth_timer_inserts);
        debug!("relay flush: insert_growth_timers count={}", rows.len());
        if let Err(e) = conn.insert_growth_timers(rows) {
            warn!("relay: insert_growth_timers: {e:?}");
        }
    }
}

fn flush_enemy_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.enemy_inserts.is_empty() {
        let rows = std::mem::take(&mut batches.enemy_inserts);
        debug!("relay flush: insert_enemies count={}", rows.len());
        if let Err(e) = conn.insert_enemies(rows) {
            warn!("relay: insert_enemies: {e:?}");
        }
    }
    if !batches.enemy_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.enemy_deletes);
        debug!("relay flush: delete_enemies count={}", ids.len());
        if let Err(e) = conn.delete_enemies(ids) {
            warn!("relay: delete_enemies: {e:?}");
        }
    }
}

fn flush_player_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.player_upserts.is_empty() {
        let rows = std::mem::take(&mut batches.player_upserts);
        debug!("relay flush: upsert_players count={}", rows.len());
        if let Err(e) = conn.upsert_players(rows) {
            warn!("relay: upsert_players: {e:?}");
        }
    }
    if !batches.player_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.player_deletes);
        debug!("relay flush: delete_players count={}", ids.len());
        if let Err(e) = conn.delete_players(ids) {
            warn!("relay: delete_players: {e:?}");
        }
    }
}

fn flush_player_state_batch(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.player_state_upserts.is_empty() {
        let rows = std::mem::take(&mut batches.player_state_upserts);
        debug!("relay flush: upsert_player_states count={}", rows.len());
        if let Err(e) = conn.upsert_player_states(rows) {
            warn!("relay: upsert_player_states: {e:?}");
        }
    }
    if !batches.player_state_deletes.is_empty() {
        let ids = std::mem::take(&mut batches.player_state_deletes);
        debug!("relay flush: delete_player_states count={}", ids.len());
        if let Err(e) = conn.delete_player_states(ids) {
            warn!("relay: delete_player_states: {e:?}");
        }
    }
}

fn flush_mobile_moves(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.mobile_moves.is_empty() {
        let moves = std::mem::take(&mut batches.mobile_moves);
        debug!("relay flush: move_mobile_entities count={}", moves.len());
        if let Err(e) = conn.move_mobile_entities(moves) {
            warn!("relay: move_mobile_entities: {e:?}");
        }
    }
}

fn flush_player_online(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.player_online_ids.is_empty() {
        let ids = std::mem::take(&mut batches.player_online_ids);
        debug!("relay flush: set_players_online count={}", ids.len());
        if let Err(e) = conn.set_players_online(ids) {
            warn!("relay: set_players_online: {e:?}");
        }
    }
}

fn flush_player_offline(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.player_offline_ids.is_empty() {
        let ids = std::mem::take(&mut batches.player_offline_ids);
        debug!("relay flush: set_players_offline count={}", ids.len());
        if let Err(e) = conn.set_players_offline(ids) {
            warn!("relay: set_players_offline: {e:?}");
        }
    }
}

fn flush_player_renames(conn: &RelayConnection, batches: &mut Batches) {
    if !batches.player_renames.is_empty() {
        let renames = std::mem::take(&mut batches.player_renames);
        debug!("relay flush: rename_players count={}", renames.len());
        if let Err(e) = conn.rename_players(renames) {
            warn!("relay: rename_players: {e:?}");
        }
    }
}
