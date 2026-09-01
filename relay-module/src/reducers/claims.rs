use crate::reducers::ensure_relay;
use crate::tables::claims::{
    ClaimInfo, ClaimMember, ClaimMeta, ClaimSupply, claim_info, claim_member, claim_meta,
    claim_supply,
};
use spacetimedb::{ReducerContext, SpacetimeType, Table, reducer};

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

/// A single field of a `claim_info` row that can change independently of the
/// others (name, bank/marketplace/waystone presence, learned research).
#[derive(SpacetimeType)]
pub enum ClaimInfoField {
    Name(String),
    Bank(bool),
    Marketplace(bool),
    Waystone(bool),
    Research(Vec<i32>),
}

/// A targeted update to one field of an existing `claim_info` row.
#[derive(SpacetimeType)]
pub struct ClaimInfoUpdate {
    pub entity_id: u64,
    pub field: ClaimInfoField,
}

/// Live-phase: apply targeted field updates to `claim_info` rows. No-ops for
/// entity_ids not already present (rows are created by `bulk_replace_claims`
/// on the sync→live transition).
#[reducer]
pub fn update_claim_info(
    ctx: &ReducerContext,
    updates: Vec<ClaimInfoUpdate>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        let Some(mut row) = ctx.db.claim_info().entity_id().find(update.entity_id) else {
            continue;
        };
        match update.field {
            ClaimInfoField::Name(name) => row.name = name,
            ClaimInfoField::Bank(bank) => row.bank = bank,
            ClaimInfoField::Marketplace(marketplace) => row.marketplace = marketplace,
            ClaimInfoField::Waystone(waystone) => row.waystone = waystone,
            ClaimInfoField::Research(research) => row.research = research,
        }
        ctx.db.claim_info().entity_id().update(row);
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

/// Live-phase: a claim was removed upstream — drop it from all claim tables,
/// including every membership row that pointed at it.
#[reducer]
pub fn delete_claims(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.claim_meta().entity_id().delete(id);
        ctx.db.claim_info().entity_id().delete(id);
        ctx.db.claim_supply().entity_id().delete(id);
        ctx.db.claim_member().by_claim().delete(id);
    }
    Ok(())
}

/// Replace the entire claim-membership set for a region in one transaction.
/// Used on the sync→live transition, mirroring `bulk_replace_claims`.
#[reducer]
pub fn bulk_replace_claim_members(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<ClaimMember>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_claim_members for region {:?}: members={}",
        region_id,
        rows.len(),
    );
    ctx.db.claim_member().by_region().delete(region_id);
    for row in rows {
        ctx.db.claim_member().insert(row);
    }
    Ok(())
}

/// Live-phase: upsert membership rows whose permissions (or claim/player
/// association) changed, or that were newly added upstream.
#[reducer]
pub fn upsert_claim_members(ctx: &ReducerContext, rows: Vec<ClaimMember>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.claim_member().entity_id().insert_or_update(row);
    }
    Ok(())
}

/// Live-phase: a member left / was removed from a claim upstream.
#[reducer]
pub fn delete_claim_members(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.claim_member().entity_id().delete(id);
    }
    Ok(())
}

/// A claim's ownership as of upstream's latest `claim_state` row.
#[derive(SpacetimeType)]
pub struct ClaimOwnerUpdate {
    pub claim_entity_id: u64,
    /// `claim_state.owner_player_entity_id`; 0 when unknown/unowned.
    pub owner_player_entity_id: u64,
}

/// Live-phase: a claim changed hands. Rather than make prism track every
/// membership row to work out which `owner` flags moved, the claim's members
/// are re-scanned here and each row's flag is set from the new owner id. A
/// membership row that does not exist yet carries the right flag from the
/// `upsert_claim_members` call that creates it.
#[reducer]
pub fn set_claim_owners(
    ctx: &ReducerContext,
    updates: Vec<ClaimOwnerUpdate>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        for mut row in ctx
            .db
            .claim_member()
            .by_claim()
            .filter(update.claim_entity_id)
        {
            let is_owner = row.player_entity_id == update.owner_player_entity_id;
            if row.owner != is_owner {
                row.owner = is_owner;
                ctx.db.claim_member().entity_id().update(row);
            }
        }
    }
    Ok(())
}
