use spacetimedb::table;

/// Static-ish per-claim metadata: location and core building descriptor.
/// Replaced wholesale per region on each sync→live transition.
#[table(accessor = claim_meta, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_building, btree(columns = [building_desc_id])),
)]
pub struct ClaimMeta {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
    pub building_desc_id: i32,
}

/// Map info about claims which may change infrequently during runtime.
/// Upserted incrementally during the live phase.
#[table(accessor = claim_info, public,
    index(accessor = by_region, btree(columns = [region_id])),
)]
pub struct ClaimInfo {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub region_id: u8,
    pub bank: bool,
    pub marketplace: bool,
    pub waystone: bool,
    pub research: Vec<i32>,
    #[default("")]
    pub name: String,
}

/// Frequently-updated per-claim supply/upkeep numbers.
/// Upserted incrementally during the live phase.
#[table(accessor = claim_supply, public,
    index(accessor = by_region, btree(columns = [region_id])),
)]
pub struct ClaimSupply {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub region_id: u8,
    pub supplies: i32,
    pub num_tiles: u32,
    pub num_tile_neighbors: u32,
    pub building_maintenance: f32,
}

/// Per-claim membership plus the upstream permission flags
/// (`claim_member_state`). Keyed per-region like the other claim tables so a
/// region's rows can be replaced wholesale on the sync→live transition.
#[table(accessor = claim_member, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_claim, btree(columns = [claim_entity_id])),
    index(accessor = by_player, btree(columns = [player_entity_id])),
)]
pub struct ClaimMember {
    /// `claim_member_state.entity_id` - currently unused (could be `autoinc`), but keep in case
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub region_id: u8,
    pub claim_entity_id: u64,
    pub player_entity_id: u64,
    pub build: bool,
    pub inventory: bool,
    pub officer: bool,
    pub co_owner: bool,
    /// Derived from `claim_state.owner_player_entity_id`.
    pub owner: bool,
}
