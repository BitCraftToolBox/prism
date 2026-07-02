//! Pipeline-specific transforms: turn a [`RegionUpdate`] into sink messages.
//!
//! During the initial subscription sync (Phase::Syncing) updates flow in as
//! the server sends the initial snapshot rows, but not all pipelines have
//! been applied yet — so the data is incomplete.  We update our join maps!
//! throughout, but defer all relay/history emission until the first
//! Phase::Live update, at which point we emit coherent bulk Replace messages
//! covering everything accumulated so far.

use super::ProcessorHandle;
use super::join::{ClaimLocalData, EntityLocation, JoinState, ProgressiveCraftState};
use crate::history::HistoryMsg;
use crate::relay::{
    ClaimInfoRow, ClaimSupplyRow, CraftContributionDeltaRow, CraftPublicUpdateRow, CraftUpdateRow,
    EnemyRow, GrowthTimerRow, PlayerRow, PlayerStateRow, RecipeMetaRow, RelayMsg, ResourceRow,
};
use crate::upstream::{Phase, RegionUpdate};
use anyhow::Result;
use log::{debug, info};
use std::collections::HashSet;
use upstream_bindings::region::DbUpdate;

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
        reducer,
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
        let timestamp_micros = event_timestamp_micros(&reducer);
        let res = region.snapshot_resources(region_id);
        let growth = region.snapshot_growth_timers(region_id);
        let enemy = region.snapshot_enemies(region_id);
        let play = region.snapshot_players(region_id);
        let player_states = region.snapshot_player_states(region_id);
        let recipe_rows = region.snapshot_recipe_meta();
        let crafts = region.snapshot_crafts(region_id, timestamp_micros);
        let claim_meta = region.snapshot_claim_meta(region_id);
        let claim_info = region.snapshot_claim_info(region_id);
        let claim_supply = region.snapshot_claim_supply(region_id);
        let history_recipe_rows = recipe_rows.clone();
        let history_crafts = crafts.clone();
        info!(
            "initial snapshot ready — emitting bulk replace: region_id={} resources={} growth_timers={} enemies={} players={} player_states={} recipes={} crafts={} claims={}",
            region_id,
            res.len(),
            growth.len(),
            enemy.len(),
            play.len(),
            player_states.len(),
            recipe_rows.len(),
            crafts.len(),
            claim_meta.len(),
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
                RelayMsg::ReplaceCrafts {
                    region_id,
                    recipe_rows,
                    rows: crafts,
                },
                RelayMsg::ReplaceClaims {
                    region_id,
                    meta_rows: claim_meta,
                    info_rows: claim_info,
                    supply_rows: claim_supply,
                },
            ]
            .into_iter(),
        );
        send_history(
            &sinks.history_tx,
            [
                HistoryMsg::UpsertRecipeMeta(history_recipe_rows),
                HistoryMsg::UpsertCrafts(history_crafts),
            ]
            .into_iter(),
        );
        // We still skip initial player-location samples because initial
        // subscription rows can be stale (e.g. offline player locations).
        // Craft/recipe state is mirrored above from the initial snapshot.

        // The snapshot is done; all sync-phase caches are no longer needed.
        // clear_live_caches seeds player_entity_ids and drops the rest.
        region.clear_live_caches();

        // The snapshot already covered this batch's state; nothing more to emit.
        return Ok(());
    }

    // Normal delta mode: emit incremental upserts/deletes derived from this batch.
    emit_deltas(region_id, &update, &reducer, region, sinks);
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

    // Claim auxiliary-building & research caches are needed in both phases to
    // emit coherent ClaimInfo rows (a row bundles bank/marketplace/waystone/
    // research together), so maintain them regardless of phase. A claim_state
    // delete that is not immediately reinserted purges every claim cache.
    for e in &update.claim_state.deletes {
        let eid = e.row.entity_id;
        if !update
            .claim_state
            .inserts
            .iter()
            .any(|i| i.row.entity_id == eid)
        {
            region.claim_research.remove(&eid);
            region.claim_banks.remove(&eid);
            region.claim_marketplaces.remove(&eid);
            region.claim_waystones.remove(&eid);
            region.claim_local.remove(&eid);
        }
    }
    for e in &update.claim_tech_state.deletes {
        region.claim_research.remove(&e.row.entity_id);
    }
    for e in &update.claim_tech_state.inserts {
        region
            .claim_research
            .insert(e.row.entity_id, e.row.learned.clone());
    }
    for e in &update.bank_state.deletes {
        region.claim_banks.remove(&e.row.claim_entity_id);
    }
    for e in &update.bank_state.inserts {
        region.claim_banks.insert(e.row.claim_entity_id);
    }
    for e in &update.marketplace_state.deletes {
        region.claim_marketplaces.remove(&e.row.claim_entity_id);
    }
    for e in &update.marketplace_state.inserts {
        region.claim_marketplaces.insert(e.row.claim_entity_id);
    }
    for e in &update.waystone_state.deletes {
        region.claim_waystones.remove(&e.row.claim_entity_id);
    }
    for e in &update.waystone_state.inserts {
        region.claim_waystones.insert(e.row.claim_entity_id);
    }

    if !live {
        for e in &update.user_state.deletes {
            region.user_identity_map.remove(&e.row.identity);
        }
        for e in &update.user_state.inserts {
            region
                .user_identity_map
                .insert(e.row.identity, e.row.entity_id);
        }
        for e in &update.building_state.deletes {
            region.building_claim_map.remove(&e.row.entity_id);
        }
        for e in &update.building_state.inserts {
            region
                .building_claim_map
                .insert(e.row.entity_id, e.row.claim_entity_id);
        }
        for e in &update.crafting_recipe_desc.deletes {
            region.recipe_map.remove(&e.row.id);
        }
        for e in &update.crafting_recipe_desc.inserts {
            region.recipe_map.insert(e.row.id, e.row.clone());
        }
        for e in &update.public_progressive_action_state.deletes {
            region.public_craft_ids.remove(&e.row.entity_id);
        }
        for e in &update.public_progressive_action_state.inserts {
            region.public_craft_ids.insert(e.row.entity_id);
        }
        for e in &update.progressive_action_state.deletes {
            region.progressive_crafts.remove(&e.row.entity_id);
        }
        for e in &update.progressive_action_state.inserts {
            region.progressive_crafts.insert(
                e.row.entity_id,
                ProgressiveCraftState {
                    entity_id: e.row.entity_id,
                    building_entity_id: e.row.building_entity_id,
                    progress: e.row.progress,
                    recipe_id: e.row.recipe_id,
                    craft_count: e.row.craft_count,
                    owner_entity_id: e.row.owner_entity_id,
                },
            );
        }

        // Claim local state drives ClaimMeta + ClaimSupply snapshots. Only
        // needed in the sync phase; live-phase rows are derived from the batch.
        for e in &update.claim_local_state.deletes {
            region.claim_local.remove(&e.row.entity_id);
        }
        for e in &update.claim_local_state.inserts {
            region
                .claim_local
                .insert(e.row.entity_id, ClaimLocalData::from_row(&e.row));
        }

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
        for e in &update.user_state.deletes {
            region.user_identity_map.remove(&e.row.identity);
        }
        for e in &update.user_state.inserts {
            region
                .user_identity_map
                .insert(e.row.identity, e.row.entity_id);
        }
        for e in &update.building_state.deletes {
            region.building_claim_map.remove(&e.row.entity_id);
        }
        for e in &update.building_state.inserts {
            region
                .building_claim_map
                .insert(e.row.entity_id, e.row.claim_entity_id);
        }

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
    reducer: &upstream_bindings::sdk::Event<upstream_bindings::region::Reducer>,
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
    let mut recipe_upserts: Vec<RecipeMetaRow> = Vec::new();
    let mut recipe_deletes: Vec<i32> = Vec::new();
    let mut craft_upserts: Vec<CraftUpdateRow> = Vec::new();
    let mut craft_public_updates: Vec<CraftPublicUpdateRow> = Vec::new();
    let mut craft_progress_deltas: Vec<CraftContributionDeltaRow> = Vec::new();
    let mut craft_expiry_ids: Vec<u64> = Vec::new();
    let mut claim_supply_upserts: Vec<ClaimSupplyRow> = Vec::new();
    let mut claim_info_upserts: Vec<ClaimInfoRow> = Vec::new();
    let mut claim_deletes: Vec<u64> = Vec::new();
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

    // Recipe metadata mirroring.
    for e in &update.crafting_recipe_desc.inserts {
        recipe_upserts.push(RecipeMetaRow {
            id: e.row.id,
            effort_required: e.row.actions_required,
            skill_id: e
                .row
                .level_requirements
                .first()
                .map(|r| r.skill_id)
                .unwrap_or(0),
            exp_per_progress: e
                .row
                .experience_per_progress
                .first()
                .map(|e| e.quantity)
                .unwrap_or(0f32),
            level_required: e
                .row
                .level_requirements
                .first()
                .map(|r| r.level)
                .unwrap_or(0),
        });
    }
    for e in &update.crafting_recipe_desc.deletes {
        if !update
            .crafting_recipe_desc
            .inserts
            .iter()
            .any(|ins| ins.row.id == e.row.id)
        {
            recipe_deletes.push(e.row.id);
        }
    }

    // Public toggles without a corresponding progressive_action_state change.
    for e in &update.public_progressive_action_state.inserts {
        let eid = e.row.entity_id;
        let has_progressive_change = has_progressive_action_state_change(update, eid);
        if !has_progressive_change {
            craft_public_updates.push(CraftPublicUpdateRow {
                craft_id: eid,
                public: true,
            });
        }
    }
    for e in &update.public_progressive_action_state.deletes {
        let eid = e.row.entity_id;
        let has_progressive_change = has_progressive_action_state_change(update, eid);
        if !has_progressive_change {
            craft_public_updates.push(CraftPublicUpdateRow {
                craft_id: eid,
                public: false,
            });
        }
    }

    // Progressive action deltas drive craft lifecycle + contribution accounting.
    let caller_player_id = match reducer {
        upstream_bindings::sdk::Event::Reducer(ev) => {
            region.user_identity_map.get(&ev.caller_identity).copied()
        }
        _ => None,
    };
    for e in &update.progressive_action_state.inserts {
        let update_timestamp_micros = event_timestamp_micros(reducer);
        let craft_id = e.row.entity_id;
        if let Some(prev) = update
            .progressive_action_state
            .deletes
            .iter()
            .find(|del| del.row.entity_id == craft_id)
        {
            let delta = e.row.progress - prev.row.progress;
            if delta != 0
                && let Some(player_id) = caller_player_id
            {
                craft_progress_deltas.push(CraftContributionDeltaRow {
                    craft_id,
                    player_id,
                    progress_delta: delta,
                    progress_total: e.row.progress,
                    last_seen_micros: update_timestamp_micros,
                });
            }
        } else {
            let public = update
                .public_progressive_action_state
                .inserts
                .iter()
                .any(|pub_row| pub_row.row.entity_id == craft_id);
            craft_upserts.push(CraftUpdateRow {
                entity_id: craft_id,
                owner_entity_id: e.row.owner_entity_id,
                claim_entity_id: region
                    .building_claim_map
                    .get(&e.row.building_entity_id)
                    .copied()
                    .unwrap_or(0),
                building_entity_id: e.row.building_entity_id,
                first_seen_micros: update_timestamp_micros,
                recipe_id: e.row.recipe_id,
                count: e.row.craft_count,
                region_id,
                public,
                progress: e.row.progress,
                last_seen_micros: update_timestamp_micros,
            });
        }
    }
    for e in &update.progressive_action_state.deletes {
        if !update
            .progressive_action_state
            .inserts
            .iter()
            .any(|ins| ins.row.entity_id == e.row.entity_id)
        {
            craft_expiry_ids.push(e.row.entity_id);
        }
    }

    // Claim deletions: a claim_state delete without a matching insert means the
    // claim is gone — drop it from all three relay tables. Compute this first
    // so supply/info upserts can skip claims that are being deleted.
    for e in &update.claim_state.deletes {
        if !update
            .claim_state
            .inserts
            .iter()
            .any(|ins| ins.row.entity_id == e.row.entity_id)
        {
            claim_deletes.push(e.row.entity_id);
        }
    }
    let claim_delete_set: HashSet<u64> = claim_deletes.iter().copied().collect();

    // ClaimSupply upserts. `claim_local_state` is one of the hottest tables in
    // the game because `xp_gained_since_last_coin_minting` changes constantly.
    // ClaimLocalData deliberately omits that field, so an update whose tracked
    // fields are unchanged compares equal and is short-circuited here.
    for e in &update.claim_local_state.inserts {
        let eid = e.row.entity_id;
        if claim_delete_set.contains(&eid) {
            continue;
        }
        let new = ClaimLocalData::from_row(&e.row);
        if let Some(prev) = update
            .claim_local_state
            .deletes
            .iter()
            .find(|d| d.row.entity_id == eid)
            && ClaimLocalData::from_row(&prev.row) == new
        {
            // Update touched only untracked fields (e.g. xp) — ignore.
            continue;
        }
        claim_supply_upserts.push(ClaimSupplyRow {
            entity_id: eid,
            region_id,
            supplies: new.supplies,
            num_tiles: new.num_tiles,
            num_tile_neighbors: new.num_tile_neighbors,
            building_maintenance: new.building_maintenance,
        });
    }

    // ClaimInfo upserts: a change to a claim's auxiliary buildings or research
    // requires re-emitting the whole ClaimInfo row (built from the caches that
    // update_join_maps has already applied for this batch).
    let mut affected_claims: HashSet<u64> = HashSet::new();
    for e in &update.claim_tech_state.inserts {
        affected_claims.insert(e.row.entity_id);
    }
    for e in &update.claim_tech_state.deletes {
        affected_claims.insert(e.row.entity_id);
    }
    for e in &update.bank_state.inserts {
        affected_claims.insert(e.row.claim_entity_id);
    }
    for e in &update.bank_state.deletes {
        affected_claims.insert(e.row.claim_entity_id);
    }
    for e in &update.marketplace_state.inserts {
        affected_claims.insert(e.row.claim_entity_id);
    }
    for e in &update.marketplace_state.deletes {
        affected_claims.insert(e.row.claim_entity_id);
    }
    for e in &update.waystone_state.inserts {
        affected_claims.insert(e.row.claim_entity_id);
    }
    for e in &update.waystone_state.deletes {
        affected_claims.insert(e.row.claim_entity_id);
    }
    for eid in affected_claims {
        if claim_delete_set.contains(&eid) {
            continue;
        }
        claim_info_upserts.push(region.claim_info_row(eid, region_id));
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
    if !recipe_upserts.is_empty() {
        history_msgs.push(HistoryMsg::UpsertRecipeMeta(recipe_upserts.clone()));
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::UpsertRecipeMeta(recipe_upserts)),
        );
    }
    if !recipe_deletes.is_empty() {
        history_msgs.push(HistoryMsg::DeleteRecipeMeta(recipe_deletes.clone()));
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::DeleteRecipeMeta(recipe_deletes)),
        );
    }
    if !craft_upserts.is_empty() {
        history_msgs.push(HistoryMsg::UpsertCrafts(craft_upserts.clone()));
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::UpsertCrafts(craft_upserts)),
        );
    }
    if !craft_public_updates.is_empty() {
        history_msgs.push(HistoryMsg::ToggleCraftPublic(craft_public_updates.clone()));
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::ToggleCraftPublic(craft_public_updates)),
        );
    }
    if !craft_progress_deltas.is_empty() {
        history_msgs.push(HistoryMsg::ApplyCraftProgressDeltas(
            craft_progress_deltas.clone(),
        ));
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::ApplyCraftProgressDeltas(craft_progress_deltas)),
        );
    }
    if !craft_expiry_ids.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::ScheduleCraftExpiry(craft_expiry_ids)),
        );
    }
    if !claim_supply_upserts.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::UpsertClaimSupply(claim_supply_upserts)),
        );
    }
    if !claim_info_upserts.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::UpsertClaimInfo(claim_info_upserts)),
        );
    }
    if !claim_deletes.is_empty() {
        send_relay(
            &sinks.relay_tx,
            std::iter::once(RelayMsg::DeleteClaims(claim_deletes)),
        );
    }
    send_history(&sinks.history_tx, history_msgs.into_iter());
}

fn has_progressive_action_state_change(update: &DbUpdate, eid: u64) -> bool {
    update
        .progressive_action_state
        .inserts
        .iter()
        .any(|ins| ins.row.entity_id == eid)
        || update
            .progressive_action_state
            .deletes
            .iter()
            .any(|del| del.row.entity_id == eid)
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

fn send_history(
    tx: &Option<tokio::sync::mpsc::Sender<HistoryMsg>>,
    msgs: impl Iterator<Item = HistoryMsg>,
) {
    let Some(tx) = tx else {
        return;
    };
    let mut dropped = 0usize;
    for msg in msgs {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(msg) {
            dropped += 1;
        }
    }
    if dropped > 0 {
        debug!("history channel full — dropped {} messages", dropped);
    }
}

fn event_timestamp_micros(
    reducer: &upstream_bindings::sdk::Event<upstream_bindings::region::Reducer>,
) -> i64 {
    match reducer {
        upstream_bindings::sdk::Event::Reducer(ev) => ev.timestamp.to_micros_since_unix_epoch(),
        _ => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0),
    }
}
