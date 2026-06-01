//! Prism relay SpacetimeDB module.
//!
//! Stores the current state (resources, enemies, players) for each BitCraft
//! region in a schema optimized for client-app subscriptions: indexed by
//! resource / enemy / region id rather than chunk index. Only the configured
//! relay identity (set once via [`init_relay`]) may write.

use spacetimedb::{table, reducer, Identity, ReducerContext, Table};

// ---------- tables ----------

#[table(accessor = resource_location, public,
    index(accessor = by_resource, btree(columns = [resource_id])),
    index(accessor = by_region,   btree(columns = [region_id])),
    index(accessor = by_resource_and_region,   btree(columns = [resource_id, region_id])))]
pub struct ResourceLocation {
    #[primary_key]
    pub entity_id: u64,
    pub resource_id: i32,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[table(accessor = enemy_location, public,
    index(accessor = by_enemy_type, btree(columns = [enemy_type])),
    index(accessor = by_region,   btree(columns = [region_id])),
    index(accessor = by_enemy_type_and_region,   btree(columns = [enemy_type, region_id])))]
pub struct EnemyLocation {
    #[primary_key]
    pub entity_id: u64,
    pub enemy_type: i32,
    pub x: i32,
    pub z: i32,
    pub region_id: u8,
}

#[table(accessor = player_location, public,
    index(accessor = by_region,   btree(columns = [region_id])))]
pub struct PlayerLocation {
    #[primary_key]
    pub entity_id: u64,
    pub x: i32,
    pub z: i32,
    pub region_id: u8,
}

/// Single-row config table holding the identity of the authorized relay client.
#[table(accessor = relay_config)]
pub struct RelayConfig {
    #[primary_key]
    pub id: u8, // always 0
    pub identity: Identity,
}

// ---------- helpers ----------

fn ensure_relay(ctx: &ReducerContext) -> Result<(), String> {
    match ctx.db.relay_config().id().find(0) {
        Some(cfg) if cfg.identity == ctx.sender() => Ok(()),
        Some(_) => Err("caller is not the configured relay identity".into()),
        None => Err("relay identity is not yet configured; call init_relay first".into()),
    }
}

// ---------- reducers ----------

/// First-call-wins initialization: stores the caller's identity as the relay.
/// After this any other reducer rejects calls from other identities.
#[reducer]
pub fn init_relay(ctx: &ReducerContext) -> Result<(), String> {
    if let Some(cfg) = ctx.db.relay_config().id().find(0) {
        if cfg.identity == ctx.sender() {
            return Ok(());
        }
        return Err("relay identity already configured".into());
    }
    ctx.db.relay_config().insert(RelayConfig { id: 0, identity: ctx.sender() });
    spacetimedb::log::debug!("relay: initialized with identity {:?}", ctx.sender());
    Ok(())
}

// --- resources ---

#[reducer]
pub fn bulk_replace_resources(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<ResourceLocation>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!("relay: processing bulk_replace_resources for region {:?}: {:?} rows", region_id, rows.len());
    ctx.db.resource_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.resource_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_resources(ctx: &ReducerContext, rows: Vec<ResourceLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.resource_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_resources(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.resource_location().entity_id().delete(&id);
    }
    Ok(())
}

// --- enemies ---

#[reducer]
pub fn bulk_replace_enemies(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<EnemyLocation>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!("relay: processing bulk_replace_enemies for region {:?}: {:?} rows", region_id, rows.len());
    ctx.db.enemy_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.enemy_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_enemies(ctx: &ReducerContext, rows: Vec<EnemyLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.enemy_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_enemies(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.enemy_location().entity_id().delete(&id);
    }
    Ok(())
}

// --- players ---

#[reducer]
pub fn bulk_replace_players(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<PlayerLocation>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!("relay: processing bulk_replace_players for region {:?}: {:?} rows", region_id, rows.len());
    ctx.db.player_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.player_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_players(ctx: &ReducerContext, rows: Vec<PlayerLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.player_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_players(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.player_location().entity_id().delete(&id);
    }
    Ok(())
}
