use spacetimedb::{SpacetimeType, Timestamp, table};

/// Lifecycle state of a craft, so consumers can tell a craft that is still
/// open for contributions from one whose upstream row is already gone.
///
/// Rows linger for 24h after the upstream `progressive_action_state` row is
/// deleted (see `schedule_craft_expiry`) so clients don't see crafts vanish
/// mid-session; without a status they'd read a finished craft as "ready to
/// collect" and a canceled one as "open for work" for that whole window.
#[derive(SpacetimeType, Clone, Copy, PartialEq, Eq, Debug)]
pub enum CraftStatus {
    /// Upstream row still exists; progress is live.
    Active,
    /// Craft was collected (`craft_collect` / `craft_collect_all`).
    Claimed,
    /// Craft disappeared for any other reason (canceled, building removed).
    Removed,
}

#[table(accessor = craft_meta, public,
    index(accessor = by_owner, btree(columns = [owner_entity_id])),
    index(accessor = by_claim, btree(columns = [claim_entity_id])),
    index(accessor = by_owner_and_claim, btree(columns = [owner_entity_id, claim_entity_id])),
    index(accessor = by_region, btree(columns = [region_id])),
)]
pub struct CraftMeta {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub owner_entity_id: u64,
    pub claim_entity_id: u64,
    pub building_entity_id: u64,
    pub first_seen: Timestamp,
    pub recipe_id: i32,
    pub count: i32,
    pub region_id: u8,
    pub public: bool,
    #[default(CraftStatus::Active)]
    pub status: CraftStatus,
}

#[table(accessor = craft_progress, public)]
pub struct CraftProgress {
    #[primary_key]
    #[index(hash)]
    pub entity_id: u64,
    pub last_seen: Timestamp,
    pub progress: i32,
}

#[table(accessor = craft_contribution, public,
    index(accessor = by_craft, btree(columns = [craft_id])),
    index(accessor = by_craft_and_player, hash(columns = [craft_id, player_id])),
)]
pub struct CraftContribution {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub craft_id: u64,
    pub player_id: u64,
    pub contribution: i32,
}

#[table(accessor = recipe_meta, public)]
pub struct RecipeMeta {
    #[primary_key]
    #[index(hash)]
    pub id: i32,
    pub effort_required: i32,
    pub skill_id: i32,
    pub exp_per_progress: f32,
    pub level_required: i32,
}
