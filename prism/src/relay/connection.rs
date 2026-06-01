//! Connection lifecycle for the downstream relay module using the standard
//! SpacetimeDB SDK 2.x (via the generated `relay_bindings` crate).
//!
//! Provides a thin wrapper around `relay_bindings::DbConnection` that handles
//! connect/reconnect/disconnect and exposes the reducers for the batcher.

use std::time::Duration;

use anyhow::{Result, anyhow};
use relay_bindings::{
    DbConnection,
    bulk_replace_enemies_reducer::bulk_replace_enemies,
    bulk_replace_players_reducer::bulk_replace_players,
    bulk_replace_resources_reducer::bulk_replace_resources,
    delete_enemies_reducer::delete_enemies,
    delete_players_reducer::delete_players,
    delete_resources_reducer::delete_resources,
    init_relay_reducer::init_relay,
    upsert_enemies_reducer::upsert_enemies,
    upsert_players_reducer::upsert_players,
    upsert_resources_reducer::upsert_resources,
    EnemyLocation, PlayerLocation, ResourceLocation,
};
use relay_sdk::DbContext;
use tokio::sync::oneshot;
use log::{info, warn};

use crate::config::RelayConfig;

pub const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// A connected relay client. Holds the connection and the pump thread handle
/// so the thread can be joined on clean shutdown.
pub struct RelayConnection {
    pub conn: DbConnection,
    pump: std::thread::JoinHandle<()>,
}

impl RelayConnection {
    /// Build and connect, call init_relay, start the message pump thread.
    /// Returns once the connection object is ready (the WebSocket handshake
    /// is asynchronous; on_connect fires once the pump processes it).
    pub async fn connect(cfg: &RelayConfig) -> Result<Self> {
        let (tx, rx) = oneshot::channel::<Result<(DbConnection, std::thread::JoinHandle<()>)>>();

        let uri = cfg.uri.clone();
        let module = cfg.module.clone();
        let token = cfg.token.clone();

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
                        warn!("relay: init_relay call failed (likely already set): {:?}", e);
                    }

                    let _ = tx.send(Ok((c, pump)));
                }
                Err(e) => {
                    let _ = tx.send(Err(anyhow!("relay connect failed: {:?}", e)));
                }
            }
        });

        let (conn, pump) = rx.await
            .map_err(|_| anyhow!("relay connect task dropped"))?
            ?;

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

    pub fn bulk_replace_resources(&self, region_id: u8, rows: Vec<ResourceLocation>) -> Result<()> {
        self.conn.reducers.bulk_replace_resources(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_enemies(&self, region_id: u8, rows: Vec<EnemyLocation>) -> Result<()> {
        self.conn.reducers.bulk_replace_enemies(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_players(&self, region_id: u8, rows: Vec<PlayerLocation>) -> Result<()> {
        self.conn.reducers.bulk_replace_players(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_resources(&self, rows: Vec<ResourceLocation>) -> Result<()> {
        self.conn.reducers.upsert_resources(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_enemies(&self, rows: Vec<EnemyLocation>) -> Result<()> {
        self.conn.reducers.upsert_enemies(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_players(&self, rows: Vec<PlayerLocation>) -> Result<()> {
        self.conn.reducers.upsert_players(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_resources(&self, ids: Vec<u64>) -> Result<()> {
        self.conn.reducers.delete_resources(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_enemies(&self, ids: Vec<u64>) -> Result<()> {
        self.conn.reducers.delete_enemies(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_players(&self, ids: Vec<u64>) -> Result<()> {
        self.conn.reducers.delete_players(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }
}