//! Per-region join state.
//!
//! The cacheless updates arrive as raw table-shaped deltas. To produce
//! sink-shaped rows we need to look up the *kind* of an entity (resource id,
//! enemy type, etc.) and the most recent companion row (location vs.
//! resource_state, mobile_entity vs. enemy_state, ...). These maps are
//! maintained across update batches, mirroring nodeindex's `consume()`.

use hashbrown::HashMap;
use crate::relay::{EnemyRow, PlayerRow, ResourceRow};

#[derive(Default)]
pub struct JoinState {
    pub regions: HashMap<u8, RegionJoinState>,
}

#[derive(Default)]
pub struct RegionJoinState {
    /// entity_id → resource_id
    pub resource_kind: HashMap<u64, i32>,
    /// entity_id → enemy_type
    pub enemy_kind: HashMap<u64, i32>,
    /// entity_id → (is a known player?)
    pub player: HashMap<u64, ()>,
    /// last known location for an entity, regardless of kind.
    pub last_location: HashMap<u64, EntityLocation>,
    /// False during initial subscription sync; true once all pipelines are live.
    /// While false, updates update join maps but are NOT emitted to sinks —
    /// we wait until the first Live update to emit a coherent bulk snapshot.
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
        self.resource_kind.iter().filter_map(|(&eid, &res_id)| {
            let loc = self.last_location.get(&eid)?;
            if loc.dimension != OVERWORLD_DIM { return None; }
            Some(ResourceRow { entity_id: eid, resource_id: res_id, region_id, x: loc.x, z: loc.z })
        }).collect()
    }

    /// Collect all known overworld enemies as relay rows for a bulk replace.
    pub fn snapshot_enemies(&self, region_id: u8) -> Vec<EnemyRow> {
        self.enemy_kind.iter().filter_map(|(&eid, &etype)| {
            let loc = self.last_location.get(&eid)?;
            if loc.dimension != OVERWORLD_DIM { return None; }
            Some(EnemyRow { entity_id: eid, enemy_type: etype, region_id, x: loc.x, z: loc.z })
        }).collect()
    }

    /// Collect all known overworld players as relay rows for a bulk replace.
    pub fn snapshot_players(&self, region_id: u8) -> Vec<PlayerRow> {
        self.player.keys().filter_map(|&eid| {
            let loc = self.last_location.get(&eid)?;
            if loc.dimension != OVERWORLD_DIM { return None; }
            Some(PlayerRow { entity_id: eid, region_id, x: loc.x, z: loc.z })
        }).collect()
    }
}

impl JoinState {
    pub fn new() -> Self { Self::default() }

    pub fn region(&mut self, region_id: u8) -> &mut RegionJoinState {
        self.regions.entry(region_id).or_default()
    }

    /// Reset all state for a region on reconnect.
    pub fn reset_region(&mut self, region_id: u8) {
        self.regions.insert(region_id, RegionJoinState::default());
    }
}
