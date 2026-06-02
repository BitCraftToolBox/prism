//! Pipeline-specific transforms: turn a [`RegionUpdate`] into sink messages.
//!
//! During the initial subscription sync (Phase::Syncing) updates flow in as
//! the server sends the initial snapshot rows, but not all pipelines have
//! been applied yet — so the data is incomplete.  We update our join maps!
//! throughout, but defer all relay/history emission until the first
//! Phase::Live update, at which point we emit coherent bulk Replace messages
//! covering everything accumulated so far.

use anyhow::Result;
use log::{info, warn};

use super::ProcessorHandle;
use super::join::{EntityLocation, JoinState};
use crate::history::HistoryMsg;
use crate::relay::{EnemyRow, PlayerRow, RelayMsg, ResourceRow};
use crate::upstream::{Phase, RegionUpdate};

const OVERWORLD_DIM: u32 = 1;

pub async fn handle(
    state: &mut JoinState,
    msg: RegionUpdate,
    sinks: &ProcessorHandle,
) -> Result<()> {
    let RegionUpdate {
        region_id,
        phase,
        update,
    } = msg;

    // On the first Syncing update for this region (e.g. after a reconnect),
    // wipe stale join state so we start clean.
    if matches!(phase, Phase::Syncing) && !state.regions.contains_key(&region_id) {
        state.reset_region(region_id);
    }

    // Always update join maps regardless of phase.
    update_join_maps(state.region(region_id), &update);

    // While syncing: update maps but hold all output until first Live update.
    if matches!(phase, Phase::Syncing) {
        return Ok(());
    }

    let region = state.region(region_id);

    // First Live update after syncing → emit bulk Replace for the full
    // accumulated snapshot, then switch to delta mode.
    if !region.is_live {
        region.is_live = true;
        let res = region.snapshot_resources(region_id);
        let enemy = region.snapshot_enemies(region_id);
        let play = region.snapshot_players(region_id);
        info!(
            "initial snapshot ready — emitting bulk replace: region_id={} resources={} enemies={} players={}",
            region_id,
            res.len(),
            enemy.len(),
            play.len(),
        );
        send_relay(
            &sinks.relay_tx,
            [
                RelayMsg::ReplaceResources {
                    region_id,
                    rows: res,
                },
                RelayMsg::ReplaceEnemies {
                    region_id,
                    rows: enemy,
                },
                RelayMsg::ReplacePlayers {
                    region_id,
                    rows: play.clone(),
                },
            ]
            .into_iter(),
        );
        // Skip sending HistoryMsg - initial subscription may give us stale state
        // (e.g. offline player locations), which we don't want to record
        // Instead, we'll only record the first movement we get after the live phase

        // The snapshot already covered this batch's state; nothing more to emit.
        return Ok(());
    }

    // Normal delta mode: emit incremental upserts/deletes derived from this batch.
    emit_deltas(region_id, &update, region, sinks);
    Ok(())
}

// ---------------------------------------------------------------------------
// Join-map updates (always run, regardless of phase)
// ---------------------------------------------------------------------------

fn update_join_maps(
    region: &mut super::join::RegionJoinState,
    update: &upstream_bindings::region::DbUpdate,
) {
    // Resources: location_state for coordinates, resource_state for kind.
    for e in &update.resource_state.deletes {
        region.resource_kind.remove(&e.row.entity_id);
        region.last_location.remove(&e.row.entity_id);
    }
    for e in &update.location_state.inserts {
        region.last_location.insert(
            e.row.entity_id,
            EntityLocation {
                x: e.row.x,
                z: e.row.z,
                dimension: e.row.dimension,
            },
        );
    }
    for e in &update.location_state.deletes {
        // location_state is exclusively for resource entities — no need to
        // cross-check against enemy_kind/player maps.
        region.last_location.remove(&e.row.entity_id);
    }
    for e in &update.resource_state.inserts {
        region
            .resource_kind
            .insert(e.row.entity_id, e.row.resource_id);
    }

    // Enemies: enemy_state for kind, mobile_entity_state for location.
    for e in &update.enemy_state.deletes {
        region.enemy_kind.remove(&e.row.entity_id);
        region.last_location.remove(&e.row.entity_id);
    }
    for e in &update.enemy_state.inserts {
        region
            .enemy_kind
            .insert(e.row.entity_id, e.row.enemy_type as i32);
    }

    // Players: player_state for membership, mobile_entity_state for location.
    for e in &update.player_state.deletes {
        region.player.remove(&e.row.entity_id);
        region.last_location.remove(&e.row.entity_id);
    }
    for e in &update.player_state.inserts {
        region.player.insert(e.row.entity_id, ());
    }

    // Mobile entity state provides locations for both enemies and players.
    for e in &update.mobile_entity_state.inserts {
        region.last_location.insert(
            e.row.entity_id,
            EntityLocation {
                x: e.row.location_x,
                z: e.row.location_z,
                dimension: e.row.dimension,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Delta emission (Live phase only)
// ---------------------------------------------------------------------------

fn emit_deltas(
    region_id: u8,
    update: &upstream_bindings::region::DbUpdate,
    region: &super::join::RegionJoinState,
    sinks: &ProcessorHandle,
) {
    let mut resource_upserts: Vec<ResourceRow> = Vec::new();
    let mut resource_deletes: Vec<u64> = Vec::new();
    let mut enemy_upserts: Vec<EnemyRow> = Vec::new();
    let mut enemy_deletes: Vec<u64> = Vec::new();
    let mut player_upserts: Vec<PlayerRow> = Vec::new();
    let mut player_deletes: Vec<u64> = Vec::new();
    let mut history_msgs: Vec<HistoryMsg> = Vec::new();

    // Resource deletes.
    for e in &update.resource_state.deletes {
        resource_deletes.push(e.row.entity_id);
    }

    // Resource upserts: when resource_state or location_state arrives in this
    // batch we can now emit because the other leg is already in the maps.
    for e in &update.resource_state.inserts {
        if let Some(loc) = region.last_location.get(&e.row.entity_id)
            && loc.dimension == OVERWORLD_DIM
        {
            resource_upserts.push(ResourceRow {
                entity_id: e.row.entity_id,
                resource_id: e.row.resource_id,
                region_id,
                x: loc.x,
                z: loc.z,
            });
        }
    }
    for e in &update.location_state.inserts {
        if let Some(&res_id) = region.resource_kind.get(&e.row.entity_id) {
            if resource_upserts
                .iter()
                .any(|r| r.entity_id == e.row.entity_id)
            {
                continue;
            }
            if e.row.dimension == OVERWORLD_DIM {
                resource_upserts.push(ResourceRow {
                    entity_id: e.row.entity_id,
                    resource_id: res_id,
                    region_id,
                    x: e.row.x,
                    z: e.row.z,
                });
            }
        }
    }

    // Mobile entity updates: resolve to enemy or player.
    for e in &update.mobile_entity_state.inserts {
        let dim = e.row.dimension;
        if dim != OVERWORLD_DIM {
            continue;
        }
        let eid = e.row.entity_id;
        if let Some(&etype) = region.enemy_kind.get(&eid) {
            enemy_upserts.push(EnemyRow {
                entity_id: eid,
                enemy_type: etype,
                region_id,
                x: e.row.location_x,
                z: e.row.location_z,
            });
        } else if region.player.contains_key(&eid) {
            player_upserts.push(PlayerRow {
                entity_id: eid,
                region_id,
                x: e.row.location_x,
                z: e.row.location_z,
            });
            history_msgs.push(HistoryMsg::PlayerLocation {
                entity_id: eid,
                timestamp: e.row.timestamp,
                x: e.row.location_x,
                z: e.row.location_z,
            });
        }
    }

    // Enemy deletes.
    for e in &update.enemy_state.deletes {
        enemy_deletes.push(e.row.entity_id);
    }
    // Player deletes.
    for e in &update.player_state.deletes {
        player_deletes.push(e.row.entity_id);
    }

    send_relay(
        &sinks.relay_tx,
        resource_upserts.into_iter().map(RelayMsg::UpsertResource),
    );
    send_relay(
        &sinks.relay_tx,
        resource_deletes.into_iter().map(RelayMsg::DeleteResource),
    );
    send_relay(
        &sinks.relay_tx,
        enemy_upserts.into_iter().map(RelayMsg::UpsertEnemy),
    );
    send_relay(
        &sinks.relay_tx,
        enemy_deletes.into_iter().map(RelayMsg::DeleteEnemy),
    );
    send_relay(
        &sinks.relay_tx,
        player_upserts.into_iter().map(RelayMsg::UpsertPlayer),
    );
    send_relay(
        &sinks.relay_tx,
        player_deletes.into_iter().map(RelayMsg::DeletePlayer),
    );
    for msg in history_msgs {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = sinks.history_tx.try_send(msg)
        {
            warn!("history channel full — dropping player location sample");
        }
    }
}

fn send_relay(tx: &tokio::sync::mpsc::Sender<RelayMsg>, msgs: impl Iterator<Item = RelayMsg>) {
    let mut dropped = 0usize;
    for msg in msgs {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(msg) {
            dropped += 1;
        }
    }
    if dropped > 0 {
        warn!("relay channel full — dropped {} messages", dropped);
    }
}
