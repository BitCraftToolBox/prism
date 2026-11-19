//! Connection lifecycle for the downstream relay module using the standard
//! SpacetimeDB SDK 2.x (via the generated `relay_bindings` crate).
//!
//! Provides a thin wrapper around `relay_bindings::DbConnection` that handles
//! connect/reconnect/disconnect and exposes the reducers for the batcher.

use std::time::Duration;

use anyhow::{Result, anyhow};
use log::{info, warn};
use relay_bindings::{
    ClaimInfo, ClaimInfoUpdate, ClaimMember, ClaimMeta, ClaimOwnerUpdate, ClaimSupply,
    CraftContributionDelta, CraftExpiry, CraftPublicUpdate, CraftUpdate, DbConnection,
    EnemyLocation, GrowthTimerUpdate, HerdLocation, MobileMoveUpdate, PlayerLocation,
    PlayerRenameUpdate, PlayerState, RecipeMeta, Region, ResourceLocation, ShopActiveUpdate,
    ShopClaimUpdate, ShopInfo, ShopInventory, ShopInventoryUpdate, TradeEvent, TradeOrder,
    apply_craft_progress_deltas_reducer::apply_craft_progress_deltas,
    bulk_replace_claim_members_reducer::bulk_replace_claim_members,
    bulk_replace_claims_reducer::bulk_replace_claims,
    bulk_replace_enemies_reducer::bulk_replace_enemies,
    bulk_replace_herds_reducer::bulk_replace_herds,
    bulk_replace_player_states_reducer::bulk_replace_player_states,
    bulk_replace_players_reducer::bulk_replace_players,
    bulk_replace_resources_reducer::bulk_replace_resources,
    bulk_replace_shop_info_reducer::bulk_replace_shop_info,
    bulk_replace_shop_inventories_reducer::bulk_replace_shop_inventories,
    bulk_replace_trade_orders_reducer::bulk_replace_trade_orders,
    delete_claim_members_reducer::delete_claim_members, delete_claims_reducer::delete_claims,
    delete_enemies_reducer::delete_enemies, delete_herds_reducer::delete_herds,
    delete_player_states_reducer::delete_player_states, delete_players_reducer::delete_players,
    delete_recipe_meta_reducer::delete_recipe_meta, delete_resources_reducer::delete_resources,
    delete_shop_info_reducer::delete_shop_info, delete_trade_orders_reducer::delete_trade_orders,
    init_relay_reducer::init_relay, insert_enemies_reducer::insert_enemies,
    insert_growth_timers_reducer::insert_growth_timers, insert_resources_reducer::insert_resources,
    insert_trade_events_reducer::insert_trade_events,
    move_mobile_entities_reducer::move_mobile_entities, reconcile_crafts_reducer::reconcile_crafts,
    rename_players_reducer::rename_players, schedule_craft_expiry_reducer::schedule_craft_expiry,
    set_claim_owners_reducer::set_claim_owners, set_players_offline_reducer::set_players_offline,
    set_players_online_reducer::set_players_online, set_shop_active_reducer::set_shop_active,
    set_shop_claims_reducer::set_shop_claims, toggle_public_reducer::toggle_public,
    update_claim_info_reducer::update_claim_info,
    update_shop_inventories_reducer::update_shop_inventories,
    upsert_claim_info_reducer::upsert_claim_info,
    upsert_claim_members_reducer::upsert_claim_members,
    upsert_claim_meta_reducer::upsert_claim_meta, upsert_claim_supply_reducer::upsert_claim_supply,
    upsert_crafts_reducer::upsert_crafts, upsert_herds_reducer::upsert_herds,
    upsert_player_states_reducer::upsert_player_states, upsert_players_reducer::upsert_players,
    upsert_recipe_meta_reducer::upsert_recipe_meta, upsert_regions_reducer::upsert_regions,
    upsert_shop_info_reducer::upsert_shop_info, upsert_trade_orders_reducer::upsert_trade_orders,
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
        let token = cfg.token.clone();

        // build() establishes the WebSocket synchronously; run in a blocking
        // thread so we don't stall the tokio executor.
        tokio::task::spawn_blocking(move || {
            let mut builder = DbConnection::builder()
                .with_uri(&uri)
                .with_database_name(&module);
            if let Some(token) = token.as_deref() {
                builder = builder.with_token(Some(token));
            }
            let result = builder
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

    pub fn bulk_replace_herds(
        &self,
        region_id: u8,
        rows: Vec<HerdLocation>,
        total: u32,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_herds(region_id, rows, total)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_herds(&self, rows: Vec<HerdLocation>) -> Result<()> {
        self.conn
            .reducers
            .upsert_herds(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_herds(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_herds(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn insert_growth_timers(&self, rows: Vec<GrowthTimerUpdate>) -> Result<()> {
        self.conn
            .reducers
            .insert_growth_timers(rows)
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

    pub fn upsert_crafts(&self, rows: Vec<CraftUpdate>) -> Result<()> {
        self.conn
            .reducers
            .upsert_crafts(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_recipe_meta(&self, rows: Vec<RecipeMeta>) -> Result<()> {
        self.conn
            .reducers
            .upsert_recipe_meta(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_recipe_meta(&self, recipe_ids: Vec<i32>) -> Result<()> {
        self.conn
            .reducers
            .delete_recipe_meta(recipe_ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn toggle_public(&self, updates: Vec<CraftPublicUpdate>) -> Result<()> {
        self.conn
            .reducers
            .toggle_public(updates)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn apply_craft_progress_deltas(&self, deltas: Vec<CraftContributionDelta>) -> Result<()> {
        self.conn
            .reducers
            .apply_craft_progress_deltas(deltas)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn schedule_craft_expiry(&self, expiries: Vec<CraftExpiry>) -> Result<()> {
        self.conn
            .reducers
            .schedule_craft_expiry(expiries)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn reconcile_crafts(&self, region_id: u8, craft_ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .reconcile_crafts(region_id, craft_ids)
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

    pub fn bulk_replace_claims(
        &self,
        region_id: u8,
        meta: Vec<ClaimMeta>,
        info: Vec<ClaimInfo>,
        supply: Vec<ClaimSupply>,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_claims(region_id, meta, info, supply)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_claim_info(&self, rows: Vec<ClaimInfo>) -> Result<()> {
        self.conn
            .reducers
            .upsert_claim_info(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn update_claim_info(&self, rows: Vec<ClaimInfoUpdate>) -> Result<()> {
        self.conn
            .reducers
            .update_claim_info(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_claim_meta(&self, rows: Vec<ClaimMeta>) -> Result<()> {
        self.conn
            .reducers
            .upsert_claim_meta(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_claim_supply(&self, rows: Vec<ClaimSupply>) -> Result<()> {
        self.conn
            .reducers
            .upsert_claim_supply(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_claims(&self, entity_ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_claims(entity_ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_claim_members(&self, region_id: u8, rows: Vec<ClaimMember>) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_claim_members(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_claim_members(&self, rows: Vec<ClaimMember>) -> Result<()> {
        self.conn
            .reducers
            .upsert_claim_members(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_claim_members(&self, entity_ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_claim_members(entity_ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn set_claim_owners(&self, updates: Vec<ClaimOwnerUpdate>) -> Result<()> {
        self.conn
            .reducers
            .set_claim_owners(updates)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_trade_orders(&self, region_id: u8, rows: Vec<TradeOrder>) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_trade_orders(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_trade_orders(&self, rows: Vec<TradeOrder>) -> Result<()> {
        self.conn
            .reducers
            .upsert_trade_orders(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_trade_orders(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_trade_orders(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn set_shop_claims(&self, updates: Vec<ShopClaimUpdate>) -> Result<()> {
        self.conn
            .reducers
            .set_shop_claims(updates)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn set_shop_active(&self, updates: Vec<ShopActiveUpdate>) -> Result<()> {
        self.conn
            .reducers
            .set_shop_active(updates)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_shop_inventories(
        &self,
        region_id: u8,
        rows: Vec<ShopInventory>,
    ) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_shop_inventories(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn update_shop_inventories(&self, updates: Vec<ShopInventoryUpdate>) -> Result<()> {
        self.conn
            .reducers
            .update_shop_inventories(updates)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn bulk_replace_shop_info(&self, region_id: u8, rows: Vec<ShopInfo>) -> Result<()> {
        self.conn
            .reducers
            .bulk_replace_shop_info(region_id, rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_shop_info(&self, rows: Vec<ShopInfo>) -> Result<()> {
        self.conn
            .reducers
            .upsert_shop_info(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn delete_shop_info(&self, ids: Vec<u64>) -> Result<()> {
        self.conn
            .reducers
            .delete_shop_info(ids)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn insert_trade_events(&self, rows: Vec<TradeEvent>) -> Result<()> {
        self.conn
            .reducers
            .insert_trade_events(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }

    pub fn upsert_regions(&self, rows: Vec<Region>) -> Result<()> {
        self.conn
            .reducers
            .upsert_regions(rows)
            .map_err(|e| anyhow!("{e:?}"))
    }
}
