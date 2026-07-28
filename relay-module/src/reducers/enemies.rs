use spacetimedb::{ReducerContext, Table, reducer};

use crate::reducers::ensure_relay;
use crate::tables::enemies::EnemyLocation;
use crate::tables::enemies::HerdLocation;
use crate::tables::enemies::enemy_location;
use crate::tables::enemies::herd_location;

#[reducer]
pub fn bulk_replace_enemies(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<EnemyLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_enemies for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.enemy_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.enemy_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn insert_enemies(ctx: &ReducerContext, rows: Vec<EnemyLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.enemy_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_enemies(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.enemy_location().entity_id().delete(id);
    }
    Ok(())
}

/// Herds are static spawners — bulk replace is used for the initial region
/// snapshot, `upsert_herds` handles new spawns.
#[reducer]
pub fn bulk_replace_herds(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<HerdLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_herds for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.herd_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.herd_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_herds(ctx: &ReducerContext, rows: Vec<HerdLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.herd_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_herds(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.herd_location().entity_id().delete(id);
    }
    Ok(())
}
