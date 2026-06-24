//! Pipeline-specific transforms: turn a [`RegionUpdate`] into sink messages.
//!
//! During the initial subscription sync (Phase::Syncing) updates flow in as
//! the server sends the initial snapshot rows, but not all pipelines have
//! been applied yet — so the data is incomplete.  We update our join maps!
//! throughout, but defer all relay/history emission until the first
//! Phase::Live update, at which point we emit coherent bulk Replace messages
//! covering everything accumulated so far.

use anyhow::Result;
use log::{debug, info, warn};

use super::ProcessorHandle;
use super::join::{EntityLocation, JoinState};
use crate::history::HistoryMsg;
use crate::relay::{EnemyRow, GrowthTimerRow, PlayerRow, PlayerStateRow, RelayMsg, ResourceRow};
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
        let growth = region.snapshot_growth_timers(region_id);
        let enemy = region.snapshot_enemies(region_id);
        let play = region.snapshot_players(region_id);
        let player_states = region.snapshot_player_states(region_id);
        info!(
            "initial snapshot ready — emitting bulk replace: region_id={} resources={} growth_timers={} enemies={} players={} player_states={}",
            region_id,
            res.len(),
            growth.len(),
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
                RelayMsg::ReplaceGrowthTimers {
                    region_id,
                    rows: growth,
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

        // The snapshot is done; all sync-phase caches are no longer needed.
        // clear_live_caches seeds player_entity_ids and drops the rest.
        region.clear_live_caches();

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

    if !live {
        // Resources: location_state for coordinates, resource_state for kind.
        for e in &update.resource_state.deletes {
            region.resource_kind.remove(&e.row.entity_id);
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
            region.last_location.remove(&e.row.entity_id);
        }
        for e in &update.resource_state.inserts {
            region
                .resource_kind
                .insert(e.row.entity_id, e.row.resource_id);
        }
        for e in &update.growth_state.inserts {
            region.growth_timers.insert(
                e.row.entity_id,
                e.row.end_timestamp.to_micros_since_unix_epoch(),
            );
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
            region.last_location.remove(&e.row.entity_id);
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
    } else {
        // Live phase: only maintain player_entity_ids for history routing.
        for e in &update.player_username_state.inserts {
            region.player_entity_ids.insert(e.row.entity_id);
        }
        for e in &update.player_username_state.deletes {
            region.player_entity_ids.remove(&e.row.entity_id);
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
    let mut growth_timer_upserts: Vec<GrowthTimerRow> = Vec::new();
    let mut player_upserts: Vec<PlayerRow> = Vec::new();
    let mut player_state_upserts: Vec<PlayerStateRow> = Vec::new();
    let mut mobile_moves: Vec<crate::relay::MobileMoveRow> = Vec::new();
    let mut player_online_ids: Vec<u64> = Vec::new();
    let mut player_offline_ids: Vec<u64> = Vec::new();
    let mut player_renames: Vec<crate::relay::PlayerRenameRow> = Vec::new();
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

    // Growth timers are keyed by resource entity_id and are insert-only here.
    // Deletes are intentionally omitted: resource deletes clear linked timers.
    for e in &update.growth_state.inserts {
        growth_timer_upserts.push(GrowthTimerRow {
            entity_id: e.row.entity_id,
            end_timestamp_micros: e.row.end_timestamp.to_micros_since_unix_epoch(),
        });
    }

    // Mobile entity updates.
    // SpacetimeDB transactional guarantees:
    //   - New enemy spawn: enemy_state.inserts + mobile_entity_state.inserts in same tx
    //   - New player arrival: player_username_state.inserts + signed_in_player_state.inserts
    //     + mobile_entity_state.inserts in same tx
    //   - Existing entity movement: only mobile_entity_state.inserts (no accompanying kind event)
    for e in &update.mobile_entity_state.inserts {
        if e.row.dimension != OVERWORLD_DIM {
            continue;
        }
        let eid = e.row.entity_id;
        let x = e.row.location_x;
        let z = e.row.location_z;

        if let Some(enemy_ins) = update
            .enemy_state
            .inserts
            .iter()
            .find(|ei| ei.row.entity_id == eid)
        {
            // New enemy spawn: we have the type from the same-batch enemy_state insert.
            enemy_upserts.push(EnemyRow {
                entity_id: eid,
                enemy_type: enemy_ins.row.enemy_type as i32,
                region_id,
                x,
                z,
            });
        } else if update
            .player_username_state
            .inserts
            .iter()
            .any(|pi| pi.row.entity_id == eid)
        {
            // New player arrival: location row only (state row is emitted below).
            player_upserts.push(PlayerRow {
                entity_id: eid,
                region_id,
                x,
                z,
            });
            history_msgs.push(HistoryMsg::PlayerLocation {
                entity_id: eid,
                timestamp: e.row.timestamp,
                x,
                z,
            });
        } else {
            // Existing entity movement — relay module resolves player vs enemy.
            mobile_moves.push(crate::relay::MobileMoveRow {
                entity_id: eid,
                region_id,
                x,
                z,
            });
            if region.player_entity_ids.contains(&eid) {
                // Dedup: skip if same large hex tile as deleted row.
                if let Some(prev) = update
                    .mobile_entity_state
                    .deletes
                    .iter()
                    .find(|ei| ei.row.entity_id == eid)
                    && prev.row.location_x / 3000 == e.row.location_x / 3000
                    && prev.row.location_z / 3000 == e.row.location_z / 3000
                {
                    continue;
                }
                history_msgs.push(HistoryMsg::PlayerLocation {
                    entity_id: eid,
                    timestamp: e.row.timestamp,
                    x: x / 3000,
                    z: z / 3000,
                });
            }
        }
    }

    // Enemy deletes come from enemy_state.
    for e in &update.enemy_state.deletes {
        enemy_deletes.push(e.row.entity_id);
    }

    // Player username events.
    //
    // Arrivals (entity not yet in player_entity_ids — update_join_maps already
    // inserted it so we re-check via the update batch rather than the set):
    //   emit UpsertPlayerState; online derived from same-batch signed_in inserts
    //   (guaranteed present if the player is currently online by game invariants).
    //
    // Renames (entity already known — update_join_maps inserted the new name
    //   before clear_live_caches ran so player_entity_ids was seeded with this id):
    //   emit RenamePlayers; no online-state change.
    //
    // Deletes → NO-OP: we don't emit DeletePlayer/DeletePlayerState in live mode
    //   to avoid racing with the receiving region on a transfer.  Stale data at
    //   last-known location/state is acceptable until the next bulk replace.
    for e in &update.player_username_state.inserts {
        let eid = e.row.entity_id;
        // Was this entity already in our set before this batch?
        // update_join_maps added it just now, so check if the pre-batch set
        // contained it by seeing if there's also a delete in this same batch
        // for the same entity_id (delete+insert == rename; insert-only == arrival).
        let is_rename = update
            .player_username_state
            .deletes
            .iter()
            .any(|d| d.row.entity_id == eid);
        if is_rename {
            player_renames.push(crate::relay::PlayerRenameRow {
                entity_id: eid,
                name: e.row.username.clone(),
            });
        } else {
            // New arrival: online iff signed_in_player_state.inserts contains this entity.
            let online = update
                .signed_in_player_state
                .inserts
                .iter()
                .any(|si| si.row.entity_id == eid);
            player_state_upserts.push(PlayerStateRow {
                entity_id: eid,
                region_id,
                online,
                name: e.row.username.clone(),
            });
        }
    }

    // Sign-in / sign-out: targeted online-field updates; no username lookup needed.
    for e in &update.signed_in_player_state.inserts {
        player_online_ids.push(e.row.entity_id);
    }
    for e in &update.signed_in_player_state.deletes {
        player_offline_ids.push(e.row.entity_id);
    }

    send_relay(
        &sinks.relay_tx,
        resource_deletes.into_iter().map(RelayMsg::DeleteResource),
    );
    send_relay(
        &sinks.relay_tx,
        resource_upserts.into_iter().map(RelayMsg::InsertResource),
    );
    send_relay(
        &sinks.relay_tx,
        growth_timer_upserts
            .into_iter()
            .map(RelayMsg::InsertGrowthTimer),
    );
    send_relay(
        &sinks.relay_tx,
        enemy_deletes.into_iter().map(RelayMsg::DeleteEnemy),
    );
    send_relay(
        &sinks.relay_tx,
        enemy_upserts.into_iter().map(RelayMsg::InsertEnemy),
    );
    send_relay(
        &sinks.relay_tx,
        player_upserts.into_iter().map(RelayMsg::UpsertPlayer),
    );
    send_relay(
        &sinks.relay_tx,
        player_state_upserts
            .into_iter()
            .map(RelayMsg::UpsertPlayerState),
    );
    if !mobile_moves.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::MoveMobileEntities(mobile_moves)),
        );
    }
    if !player_online_ids.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::SetPlayersOnline(player_online_ids)),
        );
    }
    if !player_offline_ids.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::SetPlayersOffline(player_offline_ids)),
        );
    }
    if !player_renames.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::RenamePlayers(player_renames)),
        );
    }
    for msg in history_msgs {
        if let Some(tx) = &sinks.history_tx
            && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(msg)
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
        debug!("relay channel full — dropped {} messages", dropped);
    }
}
