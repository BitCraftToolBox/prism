use spacetimedb::{ReducerContext, SpacetimeType, reducer};

use crate::reducers::ensure_relay;
use crate::tables::enemies::enemy_location;
use crate::tables::players::player_location;

/// A single mobile-entity location update. The relay module resolves whether
/// the entity is a player or an enemy by checking both tables.
#[derive(SpacetimeType)]
pub struct MobileMoveUpdate {
    pub entity_id: u64,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

/// Update the location of existing mobile entities.
/// Checks `player_location` first, then `enemy_location`. No-ops if not found.
#[reducer]
pub fn move_mobile_entities(
    ctx: &ReducerContext,
    moves: Vec<MobileMoveUpdate>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for m in moves {
        if let Some(mut row) = ctx.db.player_location().entity_id().find(m.entity_id) {
            row.x = m.x;
            row.z = m.z;
            row.region_id = m.region_id;
            ctx.db.player_location().entity_id().update(row);
        } else if let Some(mut row) = ctx.db.enemy_location().entity_id().find(m.entity_id) {
            row.x = m.x;
            row.z = m.z;
            row.region_id = m.region_id;
            ctx.db.enemy_location().entity_id().update(row);
        }
    }
    Ok(())
}
