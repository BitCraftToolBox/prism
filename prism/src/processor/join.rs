//! Per-region join state.
//!
//! The cacheless updates arrive as raw table-shaped deltas. To produce
//! sink-shaped rows we need to look up the *kind* of an entity (resource id,
//! enemy type, etc.) and the most recent companion row (location vs.
//! resource_state, mobile_entity vs. enemy_state, ...). These maps are
//! maintained across update batches, mirroring nodeindex's `consume()`.

use crate::relay::{
    ClaimInfoRow, ClaimMetaRow, ClaimSupplyRow, CraftUpdateRow, EnemyRow, GrowthTimerRow,
    PlayerRow, PlayerStateRow, RecipeMetaRow, ResourceRow,
};
use hashbrown::{HashMap, HashSet};
use upstream_bindings::region::CraftingRecipeDesc;
use upstream_bindings::sdk::Identity;

#[derive(Default)]
pub struct JoinState {
    pub regions: HashMap<u8, RegionJoinState>,
}

#[derive(Default)]
pub struct RegionJoinState {
    /// entity_id → resource_id (sync phase only; cleared by clear_live_caches)
    pub resource_kind: HashMap<u64, i32>,
    /// entity_id → enemy_type (sync phase only; cleared by clear_live_caches)
    pub enemy_kind: HashMap<u64, i32>,
    /// entity_id → username (sync phase only; cleared by clear_live_caches)
    pub player_username: HashMap<u64, String>,
    /// set of entity_ids currently signed in (sync phase only; cleared by clear_live_caches)
    pub player_signed_in: HashSet<u64>,
    /// entity_id -> growth end timestamp micros (sync phase only; cleared by clear_live_caches)
    pub growth_timers: HashMap<u64, i64>,
    /// set of entity_ids that are players in this region.
    /// Seeded from player_username.keys() at the sync→live transition, then
    /// maintained by player_username_state events.  Used in live mode to route
    /// mobile_entity movements to HistoryMsg without storing usernames.
    pub player_entity_ids: HashSet<u64>,
    /// Last known location per entity — sync phase only; cleared by clear_live_caches.
    pub last_location: HashMap<u64, EntityLocation>,
    /// False during initial subscription sync; true once all pipelines are live.
    pub is_live: bool,
    /// identity -> player entity_id (maintained in both sync and live phases).
    pub user_identity_map: HashMap<Identity, u64>,
    /// building_entity_id -> claim_entity_id (maintained in both sync and live phases).
    pub building_claim_map: HashMap<u64, u64>,
    /// recipe_id -> actions_required (sync-phase cache).
    pub recipe_map: HashMap<i32, CraftingRecipeDesc>,
    /// entity ids currently in `public_progressive_action_state` (sync-phase cache).
    pub public_craft_ids: HashSet<u64>,
    /// entity_id -> progressive craft state (sync-phase cache).
    pub progressive_crafts: HashMap<u64, ProgressiveCraftState>,
    /// claim entity_id -> local state fields for ClaimMeta/ClaimSupply.
    /// Sync-phase only (rebuilt from the batch in live phase); cleared by
    /// clear_live_caches.
    pub claim_local: HashMap<u64, ClaimLocalData>,
    /// claim entity_id -> learned research tech ids.
    ///
    /// Unlike the other claim caches this is **kept alive in the live phase**:
    /// a ClaimInfo row bundles bank/marketplace/waystone/research together, so
    /// when any one changes we need the others to emit a coherent upsert.
    pub claim_research: HashMap<u64, Vec<i32>>,
    /// claim entity_ids that currently have a bank (kept alive in live phase).
    pub claim_banks: HashSet<u64>,
    /// claim entity_ids that currently have a marketplace (kept alive in live phase).
    pub claim_marketplaces: HashSet<u64>,
    /// claim entity_ids that currently have a waystone (kept alive in live phase).
    pub claim_waystones: HashSet<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct EntityLocation {
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
}

/// The subset of `claim_local_state` fields we mirror into the relay.
/// Deliberately excludes `xp_gained_since_last_coin_minting` (and other
/// untracked fields) so we can cheaply detect no-op updates on the hot path.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClaimLocalData {
    pub x: i32,
    pub z: i32,
    pub building_desc_id: i32,
    pub supplies: i32,
    pub num_tiles: u32,
    pub num_tile_neighbors: u32,
    pub building_maintenance: f32,
}

impl ClaimLocalData {
    pub fn from_row(row: &upstream_bindings::region::ClaimLocalState) -> Self {
        let (x, z) = row.location.as_ref().map_or((0, 0), |l| (l.x, l.z));
        Self {
            x,
            z,
            building_desc_id: row.building_description_id,
            supplies: row.supplies,
            num_tiles: row.num_tiles.max(0) as u32,
            num_tile_neighbors: row.num_tile_neighbors,
            building_maintenance: row.building_maintenance,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProgressiveCraftState {
    pub entity_id: u64,
    pub building_entity_id: u64,
    pub progress: i32,
    pub recipe_id: i32,
    pub craft_count: i32,
    pub owner_entity_id: u64,
}

const OVERWORLD_DIM: u32 = 1;

impl RegionJoinState {
    /// Collect all known overworld resources as relay rows for a bulk replace.
    pub fn snapshot_resources(&self, region_id: u8) -> Vec<ResourceRow> {
        self.resource_kind
            .iter()
            .filter_map(|(&eid, &res_id)| {
                let loc = self.last_location.get(&eid)?;
                if loc.dimension != OVERWORLD_DIM {
                    return None;
                }
                Some(ResourceRow {
                    entity_id: eid,
                    resource_id: res_id,
                    region_id,
                    x: loc.x,
                    z: loc.z,
                })
            })
            .collect()
    }

    /// Collect all known overworld enemies as relay rows for a bulk replace.
    pub fn snapshot_enemies(&self, region_id: u8) -> Vec<EnemyRow> {
        self.enemy_kind
            .iter()
            .filter_map(|(&eid, &etype)| {
                let loc = self.last_location.get(&eid)?;
                if loc.dimension != OVERWORLD_DIM {
                    return None;
                }
                Some(EnemyRow {
                    entity_id: eid,
                    enemy_type: etype,
                    region_id,
                    x: loc.x,
                    z: loc.z,
                })
            })
            .collect()
    }

    /// Collect all known overworld players as relay rows for a bulk replace.
    pub fn snapshot_players(&self, region_id: u8) -> Vec<PlayerRow> {
        self.player_username
            .keys()
            .filter_map(|&eid| {
                let loc = self.last_location.get(&eid)?;
                // allow initial snapshot to record players in other dimensions
                // since it's better than just not having a location for them at all
                // if loc.dimension != OVERWORLD_DIM {
                //     return None;
                // }
                Some(PlayerRow {
                    entity_id: eid,
                    region_id,
                    x: loc.x,
                    z: loc.z,
                })
            })
            .collect()
    }

    /// Collect all known player states as relay rows for a bulk replace.
    /// Does not require last_location — derives online status from player_signed_in.
    pub fn snapshot_player_states(&self, region_id: u8) -> Vec<PlayerStateRow> {
        self.player_username
            .iter()
            .map(|(&eid, name)| PlayerStateRow {
                entity_id: eid,
                region_id,
                online: self.player_signed_in.contains(&eid),
                name: name.clone(),
            })
            .collect()
    }

    /// Collect all known growth timers for resources currently present in this region.
    pub fn snapshot_growth_timers(&self, _region_id: u8) -> Vec<GrowthTimerRow> {
        self.growth_timers
            .iter()
            .filter_map(|(&eid, &end_timestamp_micros)| {
                self.resource_kind
                    .contains_key(&eid)
                    .then_some(GrowthTimerRow {
                        entity_id: eid,
                        end_timestamp_micros,
                    })
            })
            .collect()
    }

    pub fn snapshot_recipe_meta(&self) -> Vec<RecipeMetaRow> {
        self.recipe_map
            .iter()
            .map(|recipe| RecipeMetaRow {
                id: *recipe.0,
                effort_required: recipe.1.actions_required,
                skill_id: recipe
                    .1
                    .level_requirements
                    .first()
                    .map(|r| r.skill_id)
                    .unwrap_or(0),
                exp_per_progress: recipe
                    .1
                    .experience_per_progress
                    .first()
                    .map(|s| s.quantity)
                    .unwrap_or(0f32),
                level_required: recipe
                    .1
                    .level_requirements
                    .first()
                    .map(|r| r.level)
                    .unwrap_or(0),
            })
            .collect()
    }

    pub fn snapshot_crafts(&self, region_id: u8, timestamp_micros: i64) -> Vec<CraftUpdateRow> {
        self.progressive_crafts
            .values()
            .map(|craft| CraftUpdateRow {
                entity_id: craft.entity_id,
                owner_entity_id: craft.owner_entity_id,
                claim_entity_id: self
                    .building_claim_map
                    .get(&craft.building_entity_id)
                    .copied()
                    .unwrap_or(0),
                building_entity_id: craft.building_entity_id,
                first_seen_micros: timestamp_micros,
                recipe_id: craft.recipe_id,
                count: craft.craft_count,
                region_id,
                public: self.public_craft_ids.contains(&craft.entity_id),
                progress: craft.progress,
                last_seen_micros: timestamp_micros,
            })
            .collect()
    }

    /// Collect all known claims' metadata (location + core building) as relay
    /// rows for a bulk replace.
    pub fn snapshot_claim_meta(&self, region_id: u8) -> Vec<ClaimMetaRow> {
        self.claim_local
            .iter()
            .map(|(&eid, data)| ClaimMetaRow {
                entity_id: eid,
                region_id,
                x: data.x,
                z: data.z,
                building_desc_id: data.building_desc_id,
            })
            .collect()
    }

    /// Collect all known claims' auxiliary-building/research info as relay rows.
    pub fn snapshot_claim_info(&self, region_id: u8) -> Vec<ClaimInfoRow> {
        self.claim_local
            .keys()
            .map(|&eid| ClaimInfoRow {
                entity_id: eid,
                region_id,
                bank: self.claim_banks.contains(&eid),
                marketplace: self.claim_marketplaces.contains(&eid),
                waystone: self.claim_waystones.contains(&eid),
                research: self.claim_research.get(&eid).cloned().unwrap_or_default(),
            })
            .collect()
    }

    /// Collect all known claims' supply/upkeep numbers as relay rows.
    pub fn snapshot_claim_supply(&self, region_id: u8) -> Vec<ClaimSupplyRow> {
        self.claim_local
            .iter()
            .map(|(&eid, data)| ClaimSupplyRow {
                entity_id: eid,
                region_id,
                supplies: data.supplies,
                num_tiles: data.num_tiles,
                num_tile_neighbors: data.num_tile_neighbors,
                building_maintenance: data.building_maintenance,
            })
            .collect()
    }

    /// Build a coherent ClaimInfo row for a single claim from the currently
    /// tracked caches. Used in the live phase when an auxiliary building or
    /// research changes.
    pub fn claim_info_row(&self, entity_id: u64, region_id: u8) -> ClaimInfoRow {
        ClaimInfoRow {
            entity_id,
            region_id,
            bank: self.claim_banks.contains(&entity_id),
            marketplace: self.claim_marketplaces.contains(&entity_id),
            waystone: self.claim_waystones.contains(&entity_id),
            research: self
                .claim_research
                .get(&entity_id)
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// Drop all sync-phase caches and initialize live-phase state.
    /// Called once after the initial bulk snapshot has been emitted so that
    /// delta mode carries only the minimal data needed for routing.
    ///
    /// Critically, fields are *replaced* (not `.clear()`ed) so that the
    /// backing heap allocations are freed immediately.  `.clear()` keeps the
    /// allocation alive at full capacity — for maps that peaked at 4M+ entries
    /// that is hundreds of MB of retained memory.
    pub fn clear_live_caches(&mut self) {
        // Seed player_entity_ids from the username map before dropping it.
        self.player_entity_ids = self.player_username.keys().copied().collect();
        // Replace with empty collections — this drops the old allocations.
        self.resource_kind = HashMap::default();
        self.enemy_kind = HashMap::default();
        self.player_username = HashMap::default();
        self.player_signed_in = HashSet::default();
        self.growth_timers = HashMap::default();
        self.last_location = HashMap::default();
        self.recipe_map = HashMap::default();
        self.public_craft_ids = HashSet::default();
        self.progressive_crafts = HashMap::default();
        // claim_local is only needed to build the snapshot; live-phase
        // ClaimSupply/ClaimMeta rows are derived directly from update batches.
        self.claim_local = HashMap::default();
        // claim_research / claim_banks / claim_marketplaces / claim_waystones
        // are intentionally retained: live-phase ClaimInfo upserts need the
        // full set of flags to emit a coherent row.
    }
}

impl JoinState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn region(&mut self, region_id: u8) -> &mut RegionJoinState {
        self.regions.entry(region_id).or_default()
    }

    /// Reset all state for a region on reconnect.
    pub fn reset_region(&mut self, region_id: u8) {
        self.regions.insert(region_id, RegionJoinState::default());
    }
}
