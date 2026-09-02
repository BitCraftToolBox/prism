//! Downstream relay client — connects to the Prism relay SpacetimeDB module
//! (standard SDK 2.x) and applies sink messages by calling its reducers.
//!
//! Uses latency-tiered batching: player upserts flush every ~100ms, enemies
//! every ~250ms, resources every ~1000ms.

use std::sync::Arc;

use anyhow::Result;
use log::info;
use tokio::sync::mpsc::Receiver;

use crate::config::Config;
use crate::shutdown::SharedShutdown;

pub mod batcher;
pub mod connection;

/// Default bounded-channel capacity from processor → relay.
pub fn relay_capacity(_config: &Config) -> usize {
    16384
}

#[derive(Debug, Clone)]
pub enum RelayMsg {
    ReplaceResources {
        region_id: u8,
        rows: Vec<ResourceRow>,
    },
    /// Snapshot-phase payload for growth timers in this region.
    /// This is insert-only (no table wipe) because resource replacement owns cleanup.
    ReplaceGrowthTimers {
        region_id: u8,
        rows: Vec<GrowthTimerRow>,
    },
    ReplaceEnemies {
        region_id: u8,
        rows: Vec<EnemyRow>,
    },
    /// Snapshot-phase payload for herds in this region (static spawners).
    ReplaceHerds {
        region_id: u8,
        rows: Vec<HerdRow>,
    },
    ReplacePlayers {
        region_id: u8,
        rows: Vec<PlayerRow>,
    },
    ReplacePlayerStates {
        region_id: u8,
        rows: Vec<PlayerStateRow>,
    },
    ReplaceCrafts {
        region_id: u8,
        recipe_rows: Vec<RecipeMetaRow>,
        rows: Vec<CraftUpdateRow>,
    },
    /// Snapshot-phase payload: full replacement of all claim tables for a region.
    ReplaceClaims {
        region_id: u8,
        meta_rows: Vec<ClaimMetaRow>,
        info_rows: Vec<ClaimInfoRow>,
        supply_rows: Vec<ClaimSupplyRow>,
    },
    /// Snapshot-phase payload: full replacement of `claim_member` for a region.
    ReplaceClaimMembers {
        region_id: u8,
        rows: Vec<ClaimMemberRow>,
    },

    InsertResource(ResourceRow),
    InsertGrowthTimer(GrowthTimerRow),
    InsertEnemy(EnemyRow),
    /// Live-phase delta: new herd spawn or an in-place change to a tracked
    /// herd's projected fields (ai params id / crumb-trail validity).
    UpsertHerd(HerdRow),
    UpsertPlayer(PlayerRow),
    UpsertPlayerState(PlayerStateRow),
    UpsertCrafts(Vec<CraftUpdateRow>),
    UpsertRecipeMeta(Vec<RecipeMetaRow>),
    DeleteRecipeMeta(Vec<i32>),
    ToggleCraftPublic(Vec<CraftPublicUpdateRow>),
    ApplyCraftProgressDeltas(Vec<CraftContributionDeltaRow>),
    ScheduleCraftExpiry(Vec<CraftExpiryRow>),
    /// Live-phase delta: targeted field updates to existing ClaimInfo rows
    /// (name/bank/marketplace/waystone/research), sent one field at a time.
    UpdateClaimInfo(Vec<ClaimInfoUpdate>),
    /// Live-phase delta: upsert ClaimSupply rows (supplies/tiles/upkeep).
    UpsertClaimSupply(Vec<ClaimSupplyRow>),
    /// Live-phase delta: a claim was removed upstream; drop it from all tables.
    DeleteClaims(Vec<u64>),
    /// Live-phase delta: upsert `claim_member` rows (new members, permission
    /// changes).
    UpsertClaimMembers(Vec<ClaimMemberRow>),
    /// Live-phase delta: membership rows removed upstream (member left/kicked).
    DeleteClaimMembers(Vec<u64>),
    /// Live-phase delta: claims that changed hands; the relay module re-derives
    /// the `owner` flag across each claim's membership rows.
    SetClaimOwners(Vec<ClaimOwnerRow>),
    /// Full `region` rows. Regions are few (~25 world-wide) and near-static, so
    /// both the snapshot and live-phase changes send whole rows.
    UpsertRegions(Vec<RegionRow>),

    DeleteResource(u64),
    DeleteEnemy(u64),
    DeleteHerd(u64),

    /// Live-phase delta: update location of an existing player or enemy.
    /// The relay module resolves which table to update.
    MoveMobileEntities(Vec<MobileMoveRow>),
    /// Live-phase delta: mark players as online (signed_in_player_state insert).
    SetPlayersOnline(Vec<u64>),
    /// Live-phase delta: mark players as offline (signed_in_player_state delete).
    SetPlayersOffline(Vec<u64>),
    /// Live-phase delta: rename players (player_username_state update for known entity).
    RenamePlayers(Vec<PlayerRenameRow>),
}

#[derive(Debug, Clone)]
pub struct MobileMoveRow {
    pub entity_id: u64,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[derive(Debug, Clone)]
pub struct PlayerRenameRow {
    pub entity_id: u64,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ResourceRow {
    pub entity_id: u64,
    pub resource_id: i32,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[derive(Debug, Clone)]
pub struct GrowthTimerRow {
    pub entity_id: u64,
    /// Unix-epoch timestamp in microseconds.
    pub end_timestamp_micros: i64,
}

#[derive(Debug, Clone)]
pub struct EnemyRow {
    pub entity_id: u64,
    pub enemy_type: i32,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[derive(Debug, Clone)]
pub struct HerdRow {
    pub entity_id: u64,
    pub enemy_type: i32,
    pub enemy_params_ai_desc_id: i32,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[derive(Debug, Clone)]
pub struct PlayerRow {
    pub entity_id: u64,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[derive(Debug, Clone)]
pub struct PlayerStateRow {
    pub entity_id: u64,
    pub region_id: u8,
    pub online: bool,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct RecipeMetaRow {
    pub id: i32,
    pub effort_required: i32,
    pub skill_id: i32,
    pub exp_per_progress: f32,
    pub level_required: i32,
}

#[derive(Debug, Clone)]
pub struct CraftUpdateRow {
    pub entity_id: u64,
    pub owner_entity_id: u64,
    pub claim_entity_id: u64,
    pub building_entity_id: u64,
    pub first_seen_micros: i64,
    pub recipe_id: i32,
    pub count: i32,
    pub region_id: u8,
    pub public: bool,
    pub progress: i32,
    pub last_seen_micros: i64,
}

/// Why a craft's upstream row went away. Maps onto the `Claimed`/`Removed`
/// arms of `relay_bindings::CraftStatus` — prism never expires a craft as
/// `Active`, that state is stamped by the relay module on upsert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CraftExpiryStatus {
    Claimed,
    Removed,
}

/// A craft whose upstream `progressive_action_state` row was deleted. The relay
/// module stamps `status` onto the craft and drops the row 24h later, so
/// consumers can tell a collected craft from a canceled one in the meantime.
#[derive(Debug, Clone)]
pub struct CraftExpiryRow {
    pub craft_id: u64,
    pub status: CraftExpiryStatus,
}

#[derive(Debug, Clone)]
pub struct CraftPublicUpdateRow {
    pub craft_id: u64,
    pub public: bool,
}

#[derive(Debug, Clone)]
pub struct CraftContributionDeltaRow {
    pub craft_id: u64,
    pub player_id: u64,
    pub progress_delta: i32,
    pub progress_total: i32,
    pub last_seen_micros: i64,
}

#[derive(Debug, Clone)]
pub struct ClaimMetaRow {
    pub entity_id: u64,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
    pub building_desc_id: i32,
}

#[derive(Debug, Clone)]
pub struct ClaimInfoRow {
    pub entity_id: u64,
    pub region_id: u8,
    pub name: String,
    pub bank: bool,
    pub marketplace: bool,
    pub waystone: bool,
    pub research: Vec<i32>,
}

/// A single field of a `ClaimInfo` row that can change independently of the
/// others (name, bank/marketplace/waystone presence, learned research).
#[derive(Debug, Clone)]
pub enum ClaimInfoField {
    Name(String),
    Bank(bool),
    Marketplace(bool),
    Waystone(bool),
    Research(Vec<i32>),
}

/// A targeted update to one field of an existing ClaimInfo row.
#[derive(Debug, Clone)]
pub struct ClaimInfoUpdate {
    pub entity_id: u64,
    pub field: ClaimInfoField,
}

#[derive(Debug, Clone)]
pub struct ClaimSupplyRow {
    pub entity_id: u64,
    pub region_id: u8,
    pub supplies: i32,
    pub num_tiles: u32,
    pub num_tile_neighbors: u32,
    pub building_maintenance: f32,
}

/// One `claim_member_state` row projected for the relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimMemberRow {
    /// The membership row's own entity id (not the player's, not the claim's).
    pub entity_id: u64,
    pub region_id: u8,
    pub claim_entity_id: u64,
    pub player_entity_id: u64,
    pub build: bool,
    pub inventory: bool,
    pub officer: bool,
    pub co_owner: bool,
    /// Derived from `claim_state.owner_player_entity_id` rather than read off
    /// the membership row: this player owns the claim.
    pub owner: bool,
}

/// A claim's ownership as of the latest upstream `claim_state` row.
#[derive(Debug, Clone)]
pub struct ClaimOwnerRow {
    pub claim_entity_id: u64,
    /// `claim_state.owner_player_entity_id`; 0 when unknown/unowned.
    pub owner_player_entity_id: u64,
}

/// Three upstream region tables (`world_region_name_state`,
/// `world_region_state`, `region_control_info`) collated into one relay
/// `region` row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegionRow {
    pub id: u8,
    pub name: String,
    pub min_chunk_x: u16,
    pub min_chunk_z: u16,
    pub width_chunks: u16,
    pub height_chunks: u16,
    pub initialized: bool,
    pub allow_players: bool,
    pub allow_player_spawns: bool,
}

pub async fn run(
    config: Arc<Config>,
    rx: Receiver<RelayMsg>,
    shutdown: SharedShutdown,
) -> Result<()> {
    if !config.pipelines.any_enabled() {
        info!("relay: no pipelines enabled, skipping relay subsystem");
        return Ok(());
    }
    batcher::run(config, rx, shutdown).await
}
