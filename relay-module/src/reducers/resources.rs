use crate::reducers::ensure_relay;
use crate::tables::resources::{GrowthTimer, resource_location};
use crate::tables::resources::{ResourceLocation, growth_timers};
use spacetimedb::{ReducerContext, SpacetimeType, Table, Timestamp, reducer};

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
    ctx.db.growth_timers().by_region().delete(region_id);
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
        ctx.db.growth_timers().entity_id().delete(id);
    }
    Ok(())
}

#[derive(SpacetimeType)]
pub struct GrowthTimerUpdate {
    pub entity_id: u64,
    pub end_timestamp: Timestamp,
}

#[reducer]
pub fn insert_growth_timers(
    ctx: &ReducerContext,
    timers: Vec<GrowthTimerUpdate>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for timer in timers {
        if let Some(res) = ctx.db.resource_location().entity_id().find(timer.entity_id) {
            // in theory if the game does something weird and updates a growth state for an existing resource
            // we'll get a new growth state for a resource that already had one. otherwise, we expect
            // each new growth state to correspond to a new resource without a growth state.
            // we use insert_or_update here to guard against a panic that would discard the whole tx.
            ctx.db
                .growth_timers()
                .entity_id()
                .insert_or_update(GrowthTimer {
                    entity_id: timer.entity_id,
                    resource_id: res.resource_id,
                    end_timestamp: timer.end_timestamp,
                    x: res.x,
                    z: res.z,
                    region_id: res.region_id,
                });
        }
        // it's also possible that we get a growth state for a resource entity we're not tracking
        // this generally means the resource was in a non-overworld dimension (mines, dungeons)
        // we don't care about tracking these growth timers for now anyway
    }
    Ok(())
}

// since we only have growth timers linked to resources, we don't need to directly delete them
// instead, we delete them when the linked resource is deleted
