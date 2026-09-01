use crate::reducers::ensure_relay;
use crate::tables::regions::{Region, region};
use spacetimedb::{ReducerContext, Table, reducer};

/// Upsert whole `region` rows. Each prism region task owns exactly one row
/// (its own `id`), so this doubles as both the snapshot and the live-phase
/// path.
#[reducer]
pub fn upsert_regions(ctx: &ReducerContext, rows: Vec<Region>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        spacetimedb::log::info!(
            "relay: processing upsert_regions for region {:?}: name={:?}",
            row.id,
            row.name,
        );
        ctx.db.region().id().insert_or_update(row);
    }
    Ok(())
}

/// Manual delete if data goes stale.
#[reducer]
pub fn delete_regions(ctx: &ReducerContext, regions: Option<Vec<u8>>) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!("relay: processing delete_regions for regions {:?}", regions);
    for id in regions.unwrap_or_else(|| ctx.db.region().iter().map(|r| r.id).collect()) {
        ctx.db.region().id().delete(id);
    }
    Ok(())
}
