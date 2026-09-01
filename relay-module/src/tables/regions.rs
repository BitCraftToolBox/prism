use spacetimedb::table;

/// One row per game region, collating three upstream region tables
/// (`world_region_name_state`, `world_region_state`, `region_control_info`)
/// into a single flat row.
#[table(accessor = region, public)]
pub struct Region {
    #[primary_key]
    #[index(direct)]
    pub id: u8,
    pub name: String,
    pub min_chunk_x: u16,
    pub min_chunk_z: u16,
    pub width_chunks: u16,
    pub height_chunks: u16,
    pub initialized: bool,
    pub allow_players: bool,
    pub allow_player_spawns: bool,
}
