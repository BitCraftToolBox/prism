use spacetimedb::{ReducerContext, SpacetimeType, reducer};

use crate::reducers::ensure_relay;
use crate::tables::players::player_location;
use crate::tables::players::player_state;
use crate::tables::players::{PlayerLocation, PlayerState};

#[reducer]
pub fn bulk_replace_players(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<PlayerLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_players for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.player_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.player_location().entity_id().insert_or_update(row);
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
        ctx.db.player_location().entity_id().delete(id);
    }
    Ok(())
}

#[reducer]
pub fn bulk_replace_player_states(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<PlayerState>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_player_states for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.player_state().by_region().delete(region_id);
    for row in rows {
        ctx.db.player_state().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_player_states(ctx: &ReducerContext, rows: Vec<PlayerState>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.player_state().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_player_states(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.player_state().entity_id().delete(id);
    }
    Ok(())
}

/// A single player rename update.
#[derive(SpacetimeType)]
pub struct PlayerRenameUpdate {
    pub entity_id: u64,
    pub name: String,
}

/// Update only the `name` field of existing `player_state` rows.
/// No-ops for entity_ids not in `player_state`.
#[reducer]
pub fn rename_players(
    ctx: &ReducerContext,
    renames: Vec<PlayerRenameUpdate>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for r in renames {
        if let Some(mut row) = ctx.db.player_state().entity_id().find(r.entity_id) {
            row.name = r.name;
            ctx.db.player_state().entity_id().update(row);
        }
    }
    Ok(())
}

/// Mark a set of players as online. No-ops for entity_ids not in `player_state`.
#[reducer]
pub fn set_players_online(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        if let Some(mut row) = ctx.db.player_state().entity_id().find(id) {
            row.online = true;
            ctx.db.player_state().entity_id().update(row);
        }
    }
    Ok(())
}

/// Mark a set of players as offline. No-ops for entity_ids not in `player_state`.
#[reducer]
pub fn set_players_offline(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        if let Some(mut row) = ctx.db.player_state().entity_id().find(id) {
            row.online = false;
            ctx.db.player_state().entity_id().update(row);
        }
    }
    Ok(())
}
