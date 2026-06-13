use spacetimedb::table;

#[table(accessor = resource_location, public,
    index(accessor = by_resource, btree(columns = [resource_id])),
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_resource_and_region, btree(columns = [resource_id, region_id])))]
pub struct ResourceLocation {
    #[primary_key]
    pub entity_id: u64,
    pub resource_id: i32,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

