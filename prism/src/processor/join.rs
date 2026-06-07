//! Per-region join state.
//!
//! The cacheless updates arrive as raw table-shaped deltas. To produce
//! sink-shaped rows we need to look up the *kind* of an entity (resource id,
//! enemy type, etc.) and the most recent companion row (location vs.
//! resource_state, mobile_entity vs. enemy_state, ...). These maps are
//! maintained across update batches, mirroring nodeindex's `consume()`.

use crate::relay::{EnemyRow, PlayerRow, PlayerStateRow, ResourceRow};
use hashbrown::{HashMap, HashSet};

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
    /// set of entity_ids that are players in this region.
    /// Seeded from player_username.keys() at the sync→live transition, then
    /// maintained by player_username_state events.  Used in live mode to route
    /// mobile_entity movements to HistoryMsg without storing usernames.
    pub player_entity_ids: HashSet<u64>,
    /// Last known location per entity — sync phase only; cleared by clear_live_caches.
    pub last_location: HashMap<u64, EntityLocation>,
    /// False during initial subscription sync; true once all pipelines are live.
    pub is_live: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct EntityLocation {
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
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
                if loc.dimension != OVERWORLD_DIM {
                    return None;
                }
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
        self.last_location = HashMap::default();
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
