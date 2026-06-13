use spacetimedb::{ReducerContext, Table, reducer};

use crate::reducers::ensure_relay;
use crate::tables::enemies::EnemyLocation;
use crate::tables::enemies::enemy_location;

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
