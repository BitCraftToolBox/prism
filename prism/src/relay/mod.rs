//! Downstream relay client — connects to the Prism relay SpacetimeDB module
//! (standard SDK 2.x) and applies sink messages by calling its reducers.
//!
//! Uses latency-tiered batching: player upserts flush every ~100ms, enemies
//! every ~250ms, resources every ~1000ms.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;

use crate::config::Config;
use crate::shutdown::SharedShutdown;

pub mod batcher;
pub mod connection;

/// Default bounded-channel capacity from processor → relay.
pub fn relay_capacity(_config: &Config) -> usize {
    8192
}

#[derive(Debug, Clone)]
pub enum RelayMsg {
    ReplaceResources { region_id: u8, rows: Vec<ResourceRow> },
    ReplaceEnemies   { region_id: u8, rows: Vec<EnemyRow> },
    ReplacePlayers   { region_id: u8, rows: Vec<PlayerRow> },

    UpsertResource(ResourceRow),
    UpsertEnemy(EnemyRow),
    UpsertPlayer(PlayerRow),

    DeleteResource(u64),
    DeleteEnemy(u64),
    DeletePlayer(u64),
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

pub async fn run(
    config: Arc<Config>,
    rx: Receiver<RelayMsg>,
    shutdown: SharedShutdown,
) -> Result<()> {
    batcher::run(config, rx, shutdown).await
}


