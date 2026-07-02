use crate::reducers::ensure_relay;
use crate::tables::claims::{
    ClaimInfo, ClaimMeta, ClaimSupply, claim_info, claim_meta, claim_supply,
};
use spacetimedb::{ReducerContext, Table, reducer};

/// Replace the entire claim state for a region in one transaction. Used on the
/// sync→live transition to publish a fresh, coherent snapshot: all three claim
/// tables are wiped for the region and repopulated from the passed rows.
#[reducer]
pub fn bulk_replace_claims(
    ctx: &ReducerContext,
    region_id: u8,
    meta: Vec<ClaimMeta>,
    info: Vec<ClaimInfo>,
    supply: Vec<ClaimSupply>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_claims for region {:?}: meta={} info={} supply={}",
        region_id,
        meta.len(),
        info.len(),
        supply.len(),
    );
    ctx.db.claim_meta().by_region().delete(region_id);
    ctx.db.claim_info().by_region().delete(region_id);
    ctx.db.claim_supply().by_region().delete(region_id);
    for row in meta {
        ctx.db.claim_meta().insert(row);
    }
    for row in info {
        ctx.db.claim_info().insert(row);
    }
    for row in supply {
        ctx.db.claim_supply().insert(row);
    }
    Ok(())
}

/// Live-phase: upsert ClaimInfo rows (bank/marketplace/waystone presence and
/// learned research) for claims whose auxiliary buildings or tech changed.
#[reducer]
pub fn upsert_claim_info(ctx: &ReducerContext, rows: Vec<ClaimInfo>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.claim_info().entity_id().insert_or_update(row);
    }
    Ok(())
}

/// Live-phase: upsert ClaimSupply rows for claims whose supply/upkeep numbers
/// changed. Callers are expected to filter out no-op updates (e.g. the hot
/// `xp_gained_since_last_coin_minting` field) before calling.
#[reducer]
pub fn upsert_claim_supply(ctx: &ReducerContext, rows: Vec<ClaimSupply>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.claim_supply().entity_id().insert_or_update(row);
    }
    Ok(())
}

/// Live-phase: a claim was removed upstream — drop it from all three tables.
#[reducer]
pub fn delete_claims(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.claim_meta().entity_id().delete(id);
        ctx.db.claim_info().entity_id().delete(id);
        ctx.db.claim_supply().entity_id().delete(id);
    }
    Ok(())
}
