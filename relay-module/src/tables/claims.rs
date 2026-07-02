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

/// Which auxiliary buildings a claim has plus its learned research.
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
