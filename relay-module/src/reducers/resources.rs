use spacetimedb::{ReducerContext, Table, reducer};

use crate::reducers::ensure_relay;
use crate::tables::resources::ResourceLocation;
use crate::tables::resources::resource_location;

#[reducer]
pub fn bulk_replace_resources(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<ResourceLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_resources for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.resource_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.resource_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn insert_resources(ctx: &ReducerContext, rows: Vec<ResourceLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.resource_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_resources(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.resource_location().entity_id().delete(id);
    }
    Ok(())
}
