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

    InsertResource(ResourceRow),
    InsertGrowthTimer(GrowthTimerRow),
    InsertEnemy(EnemyRow),
    UpsertPlayer(PlayerRow),
    UpsertPlayerState(PlayerStateRow),
    UpsertCrafts(Vec<CraftUpdateRow>),
    UpsertRecipeMeta(Vec<RecipeMetaRow>),
    DeleteRecipeMeta(Vec<i32>),
    ToggleCraftPublic(Vec<CraftPublicUpdateRow>),
    ApplyCraftProgressDeltas(Vec<CraftContributionDeltaRow>),
    ScheduleCraftExpiry(Vec<u64>),

    DeleteResource(u64),
    DeleteEnemy(u64),

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
