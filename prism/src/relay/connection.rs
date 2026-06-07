//! Connection lifecycle for the downstream relay module using the standard
//! SpacetimeDB SDK 2.x (via the generated `relay_bindings` crate).
//!
//! Provides a thin wrapper around `relay_bindings::DbConnection` that handles
//! connect/reconnect/disconnect and exposes the reducers for the batcher.

use std::time::Duration;

use anyhow::{Result, anyhow};
use log::{info, warn};
use relay_bindings::{
    DbConnection, EnemyLocation, MobileMoveUpdate, PlayerLocation, PlayerRenameUpdate, PlayerState,
    ResourceLocation, bulk_replace_enemies_reducer::bulk_replace_enemies,
    bulk_replace_player_states_reducer::bulk_replace_player_states,
    bulk_replace_players_reducer::bulk_replace_players,
    bulk_replace_resources_reducer::bulk_replace_resources, delete_enemies_reducer::delete_enemies,
    delete_player_states_reducer::delete_player_states, delete_players_reducer::delete_players,
    delete_resources_reducer::delete_resources, init_relay_reducer::init_relay,
    insert_enemies_reducer::insert_enemies, insert_resources_reducer::insert_resources,
    move_mobile_entities_reducer::move_mobile_entities, rename_players_reducer::rename_players,
    set_players_offline_reducer::set_players_offline,
    set_players_online_reducer::set_players_online,
    upsert_player_states_reducer::upsert_player_states, upsert_players_reducer::upsert_players,
};
use relay_sdk::DbContext;
use tokio::sync::oneshot;

use crate::config::RelayConfig;

pub const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// A connected relay client. Holds the connection and the pump thread handle
/// so the thread can be joined on clean shutdown.
pub struct RelayConnection {
    pub conn: DbConnection,
    pump: std::thread::JoinHandle<()>,
}

fn error_is_normal_disconnect(e: &relay_sdk::Error) -> bool {
    matches!(e, relay_sdk::Error::Disconnected)
}

impl RelayConnection {
    /// Build and connect, call init_relay, start the message pump thread.
    /// Returns once the connection object is ready (the WebSocket handshake
    /// is asynchronous; on_connect fires once the pump processes it).
    pub async fn connect(cfg: &RelayConfig) -> Result<Self> {
        let (tx, rx) = oneshot::channel::<Result<(DbConnection, std::thread::JoinHandle<()>)>>();

        let uri = cfg.uri.clone();
        let module = cfg.module.clone();
        let token = cfg
            .token
            .clone()
            .expect("Relay token missing, should have been validated in config.");

        // build() establishes the WebSocket synchronously; run in a blocking
        // thread so we don't stall the tokio executor.
        tokio::task::spawn_blocking(move || {
            let result = DbConnection::builder()
                .with_uri(&uri)
                .with_database_name(&module)
                .with_token(Some(&token))
                .on_connect(|_ctx, _id, _tok| {
                    info!("relay: connected to downstream module");
                })
                .on_disconnect(|_ctx, err| match err {
                    Some(e) if error_is_normal_disconnect(&e) => info!("relay: disconnected"),
                    Some(e) => warn!("relay: disconnected: {:?}", e),
                    None => info!("relay: disconnected"),
                })
                .build();

            match result {
                Ok(c) => {
                    // Keep the JoinHandle so we can join on shutdown.
                    let pump = c.run_threaded();

                    // init_relay is first-call-wins; errors here are benign.
                    if let Err(e) = c.reducers.init_relay() {
                        warn!(
                            "relay: init_relay call failed (likely already set): {:?}",
                            e
                        );
                    }

                    let _ = tx.send(Ok((c, pump)));
                }
                Err(e) => {
                    let _ = tx.send(Err(anyhow!("relay connect failed: {:?}", e)));
                }
            }
        });

        let (conn, pump) = rx
            .await
            .map_err(|_| anyhow!("relay connect task dropped"))??;

        Ok(Self { conn, pump })
    }

    /// Whether the underlying WebSocket connection is still alive.
    pub fn is_active(&self) -> bool {
        self.conn.is_active()
    }

    /// Disconnect the WebSocket and join the pump thread.
    pub fn disconnect(self) {
        if let Err(e) = self.conn.disconnect() {
            warn!("relay: disconnect error: {:?}", e);
        }
        // Give the pump thread a moment to notice the disconnect, then join.
        let _ = self.pump.join();
    }

    // --- Reducer wrappers ---

    pub fn bulk_replace_resources(
        &self,
        region_id: u8,
        rows: Vec<ResourceLocation>,
        total: u32,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_resources(region_id, rows, total)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_enemies(
        &self,
        region_id: u8,
        rows: Vec<EnemyLocation>,
        total: u32,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_enemies(region_id, rows, total)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_players(
        &self,
        region_id: u8,
        rows: Vec<PlayerLocation>,
        total: u32,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_players(region_id, rows, total)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn insert_resources(&self, rows: Vec<ResourceLocation>) -> Result<()> {
        self.conn
            .reducers
            .insert_resources(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn insert_enemies(&self, rows: Vec<EnemyLocation>) -> Result<()> {
        self.conn
            .reducers
            .insert_enemies(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_players(&self, rows: Vec<PlayerLocation>) -> Result<()> {
        self.conn
            .reducers
            .upsert_players(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_resources(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_resources(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_enemies(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_enemies(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_players(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_players(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_player_states(
        &self,
        region_id: u8,
        rows: Vec<PlayerState>,
        total: u32,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_player_states(region_id, rows, total)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_player_states(&self, rows: Vec<PlayerState>) -> Result<()> {
        self.conn
            .reducers
            .upsert_player_states(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_player_states(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_player_states(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn move_mobile_entities(&self, moves: Vec<MobileMoveUpdate>) -> Result<()> {
        self.conn
            .reducers
            .move_mobile_entities(moves)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn set_players_online(&self, entity_ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .set_players_online(entity_ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn set_players_offline(&self, entity_ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .set_players_offline(entity_ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn rename_players(&self, renames: Vec<PlayerRenameUpdate>) -> Result<()> {
        self.conn
            .reducers
            .rename_players(renames)
            .map_err(|e| anyhow!("{e:?}"))
    }
}
