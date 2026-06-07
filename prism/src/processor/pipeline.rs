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
use crate::relay::{EnemyRow, PlayerRow, PlayerStateRow, RelayMsg, ResourceRow};
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
        let player_states = region.snapshot_player_states(region_id);
        info!(
            "initial snapshot ready — emitting bulk replace: region_id={} resources={} enemies={} players={} player_states={}",
            region_id,
            res.len(),
            enemy.len(),
            play.len(),
            player_states.len(),
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
                    rows: play,
                },
                RelayMsg::ReplacePlayerStates {
                    region_id,
                    rows: player_states,
                },
            ]
            .into_iter(),
        );
        // Skip sending HistoryMsg - initial subscription may give us stale state
        // (e.g. offline player locations), which we don't want to record.
        // Instead, we'll only record the first movement we get after the live phase.

        // The snapshot is done; the last_location cache is no longer needed.
        // Drop it now so delta mode doesn't maintain stale per-entity history.
        region.clear_last_location();

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
    let live = region.is_live;

    // Resources: location_state for coordinates, resource_state for kind.
    for e in &update.resource_state.deletes {
        region.resource_kind.remove(&e.row.entity_id);
    }
    if !live {
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
            region.last_location.remove(&e.row.entity_id);
        }
    }
    for e in &update.resource_state.inserts {
        region
            .resource_kind
            .insert(e.row.entity_id, e.row.resource_id);
    }

    // Enemies: enemy_state for kind, mobile_entity_state for location.
    for e in &update.enemy_state.deletes {
        region.enemy_kind.remove(&e.row.entity_id);
    }
    for e in &update.enemy_state.inserts {
        region
            .enemy_kind
            .insert(e.row.entity_id, e.row.enemy_type as i32);
    }

    // Players: player_username_state for membership/username,
    //          signed_in_player_state for online status.
    for e in &update.player_username_state.deletes {
        region.player_username.remove(&e.row.entity_id);
        region.player_signed_in.remove(&e.row.entity_id);
        if !live {
            region.last_location.remove(&e.row.entity_id);
        }
    }
    for e in &update.player_username_state.inserts {
        region
            .player_username
            .insert(e.row.entity_id, e.row.username.clone());
    }
    for e in &update.signed_in_player_state.deletes {
        region.player_signed_in.remove(&e.row.entity_id);
    }
    for e in &update.signed_in_player_state.inserts {
        region.player_signed_in.insert(e.row.entity_id);
    }

    // Mobile entity state provides locations for both enemies and players.
    // Only needed during the Syncing phase for snapshot building.
    if !live {
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
    let mut player_state_upserts: Vec<PlayerStateRow> = Vec::new();
    let mut player_state_deletes: Vec<u64> = Vec::new();
    let mut history_msgs: Vec<HistoryMsg> = Vec::new();

    // Resource deletes.
    for e in &update.resource_state.deletes {
        resource_deletes.push(e.row.entity_id);
    }

    // Resource inserts: only when both resource_state and location_state arrive
    // in the same update (guaranteed by transaction boundaries).
    for e in &update.resource_state.inserts {
        if let Some(loc) = update
            .location_state
            .inserts
            .iter()
            .find(|l| l.row.entity_id == e.row.entity_id)
            && loc.row.dimension == OVERWORLD_DIM
        {
            resource_upserts.push(ResourceRow {
                entity_id: e.row.entity_id,
                resource_id: e.row.resource_id,
                region_id,
                x: loc.row.x,
                z: loc.row.z,
            });
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
        } else if region.player_username.contains_key(&eid) {
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

    // Enemy deletes come from enemy_state.
    for e in &update.enemy_state.deletes {
        enemy_deletes.push(e.row.entity_id);
    }

    // Player location + state deletes: player_username_state deletion means the
    // player left (or transferred to another region); remove both location and state.
    for e in &update.player_username_state.deletes {
        player_deletes.push(e.row.entity_id);
        player_state_deletes.push(e.row.entity_id);
    }

    // Player state upserts on username arrival (new player in region).
    // Online status is read from the already-updated signed_in map.
    for e in &update.player_username_state.inserts {
        let eid = e.row.entity_id;
        player_state_upserts.push(PlayerStateRow {
            entity_id: eid,
            region_id,
            online: region.player_signed_in.contains(&eid),
            name: e.row.username.clone(),
        });
    }

    // Player state upserts on sign-in / sign-out (online flag changes).
    for e in &update.signed_in_player_state.inserts {
        let eid = e.row.entity_id;
        if let Some(name) = region.player_username.get(&eid) {
            player_state_upserts.push(PlayerStateRow {
                entity_id: eid,
                region_id,
                online: true,
                name: name.clone(),
            });
        }
    }
    for e in &update.signed_in_player_state.deletes {
        let eid = e.row.entity_id;
        if let Some(name) = region.player_username.get(&eid) {
            player_state_upserts.push(PlayerStateRow {
                entity_id: eid,
                region_id,
                online: false,
                name: name.clone(),
            });
        }
    }

    send_relay(
        &sinks.relay_tx,
        resource_deletes.into_iter().map(RelayMsg::DeleteResource),
    );
    send_relay(
        &sinks.relay_tx,
        resource_upserts.into_iter().map(RelayMsg::UpsertResource),
    );
    send_relay(
        &sinks.relay_tx,
        enemy_deletes.into_iter().map(RelayMsg::DeleteEnemy),
    );
    send_relay(
        &sinks.relay_tx,
        enemy_upserts.into_iter().map(RelayMsg::UpsertEnemy),
    );
    send_relay(
        &sinks.relay_tx,
        player_deletes.into_iter().map(RelayMsg::DeletePlayer),
    );
    send_relay(
        &sinks.relay_tx,
        player_upserts.into_iter().map(RelayMsg::UpsertPlayer),
    );
    send_relay(
        &sinks.relay_tx,
        player_state_deletes
            .into_iter()
            .map(RelayMsg::DeletePlayerState),
    );
    send_relay(
        &sinks.relay_tx,
        player_state_upserts
            .into_iter()
            .map(RelayMsg::UpsertPlayerState),
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
