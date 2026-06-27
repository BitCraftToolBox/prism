use spacetimedb::table;

#[table(accessor = enemy_location, public,
    index(accessor = by_enemy_type, btree(columns = [enemy_type])),
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_enemy_type_and_region, btree(columns = [enemy_type, region_id]))
)]
pub struct EnemyLocation {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub enemy_type: i32,
    pub x: i32,
    pub z: i32,
    pub region_id: u8,
}
