use spacetimedb::table;

#[table(accessor = player_location, public,
    index(accessor = by_region, btree(columns = [region_id]))
)]
pub struct PlayerLocation {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub x: i32,
    pub z: i32,
    pub region_id: u8,
}

#[table(accessor = player_state, public,
    index(accessor = by_region, btree(columns = [region_id]))
)]
pub struct PlayerState {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub region_id: u8,
    pub online: bool,
    pub name: String,
}
