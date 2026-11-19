//! Per-region join state.
//!
//! The cacheless updates arrive as raw table-shaped deltas. To produce
//! sink-shaped rows we need to look up the *kind* of an entity (resource id,
//! enemy type, etc.) and the most recent companion row (location vs.
//! resource_state, mobile_entity vs. enemy_state, ...). These maps are
//! maintained across update batches, mirroring nodeindex's `consume()`.

use crate::relay::{
    ClaimInfoRow, ClaimMemberRow, ClaimMetaRow, ClaimSupplyRow, CraftUpdateRow, EnemyRow,
    GrowthTimerRow, HerdRow, InventoryPartKind, ItemKind, ItemStackRow, PlayerRow, PlayerStateRow,
    PocketRow, RecipeMetaRow, RegionRow, ResourceRow, ShopInfoRow, ShopInventoryRow, ShopKind,
    TradeEventRow, TradeOrderRow,
};
use hashbrown::{HashMap, HashSet};
use upstream_bindings::region::{
    BarterStallInventoryChangeReason, BarterStallInventoryEvent, CraftingRecipeDesc,
    InventoryState, ItemStack, ItemType, Pocket, TradeOrderState,
};

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
    /// entity_id → herd sync data (sync phase only; cleared by clear_live_caches)
    pub herd_kind: HashMap<u64, HerdSyncEntry>,
    /// enemy_ai_params_desc id -> enemy_type. Populated from the
    /// `enemy_ai_params_desc` subscription (Enemies pipeline) and used to
    /// resolve the initial herd snapshot plus, in the live phase, newly
    /// spawned herds (which may reference a type added by a game update).
    /// Maintained in both phases and intentionally survives clear_live_caches.
    pub enemy_ai_type_map: HashMap<i32, i32>,
    /// entity_id → username (sync phase only; cleared by clear_live_caches)
    pub player_username: HashMap<u64, String>,
    /// set of entity_ids currently signed in (sync phase only; cleared by clear_live_caches)
    pub player_signed_in: HashSet<u64>,
    /// entity_id -> growth end timestamp micros (sync phase only; cleared by clear_live_caches)
    pub growth_timers: HashMap<u64, i64>,
    /// resource_desc id -> tag. Populated from the `resource_desc` subscription
    /// (GrowthTimers pipeline) and used in the live phase to decide which newly
    /// inserted resources warrant a targeted `resource_growth_timer` sub, so it
    /// is maintained in both phases and intentionally survives clear_live_caches.
    pub resource_desc_tags: HashMap<i32, String>,
    /// set of entity_ids that are players in this region.
    /// Seeded from player_username.keys() at the sync→live transition, then
    /// maintained by player_username_state events.  Used in live mode to route
    /// mobile_entity movements to HistoryMsg without storing usernames.
    pub player_entity_ids: HashSet<u64>,
    /// Last known location per entity — sync phase only; cleared by clear_live_caches.
    pub last_location: HashMap<u64, EntityLocation>,
    /// False during initial subscription sync; true once all pipelines are live.
    pub is_live: bool,
    /// building_entity_id -> claim_entity_id (maintained in both sync and live phases).
    pub building_claim_map: HashMap<u64, u64>,
    /// building_entity_id -> building_description_id (maintained alongside
    /// `building_claim_map`); used to type the shop of a trade order.
    pub building_desc_ids: HashMap<u64, i32>,
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
    /// claim entity_id -> name (sync phase only; cleared by clear_live_caches).
    /// Live-phase renames are emitted as targeted ClaimInfo field updates
    /// directly from the update batch, so no cache is needed once live.
    pub claim_names: HashMap<u64, String>,
    /// claim entity_id -> learned research tech ids (sync phase only; cleared
    /// by clear_live_caches). Only needed to build the initial coherent
    /// ClaimInfo snapshot; live-phase changes are emitted as targeted field
    /// updates directly from the update batch.
    pub claim_research: HashMap<u64, Vec<i32>>,
    /// claim entity_ids that currently have a bank (sync phase only; cleared
    /// by clear_live_caches).
    pub claim_banks: HashSet<u64>,
    /// claim entity_ids that currently have a marketplace (sync phase only;
    /// cleared by clear_live_caches).
    pub claim_marketplaces: HashSet<u64>,
    /// claim entity_ids that currently have a waystone (sync phase only;
    /// cleared by clear_live_caches).
    pub claim_waystones: HashSet<u64>,
    /// claim entity_id -> owner player entity_id. Maintained in both phases and
    /// intentionally survives clear_live_caches: `ClaimMember::owner` is derived
    /// from `claim_state`, so the live phase needs this to stamp the flag onto
    /// membership rows as members join or their permissions change. One u64 pair
    /// per claim, so retaining it is cheap.
    pub claim_owners: HashMap<u64, u64>,
    /// claim_member_state entity_id -> membership row (sync phase only;
    /// cleared by clear_live_caches). Live-phase membership changes are
    /// emitted as upserts/deletes directly from the update batch.
    pub claim_members: HashMap<u64, ClaimMemberData>,
    /// building entity_id -> nickname (`building_nickname_state`). Maintained in
    /// both phases and intentionally survives clear_live_caches: the live phase
    /// needs it to join a shop name onto newly-inserted trade orders. Only named
    /// buildings have a row, so it stays small.
    pub building_nicknames: HashMap<u64, String>,
    /// deployable entity_id -> claim/nickname/owner/kind (`deployable_state_v2`).
    /// Maintained in both phases and retained, for the same reason as
    /// `building_nicknames`.
    pub deployable_shops: HashMap<u64, ShopMeta>,
    /// Shops in the world: `barter_stall_state` entity ids, one per barter
    /// stall building or placed trader stand (stowed stands drop out).
    /// Maintained in both phases and retained: the live phase flags new trade
    /// orders active/inactive from it and only re-sends `shop_info` rows for
    /// shops in it. A few hundred per region.
    pub barter_stalls: HashSet<u64>,
    /// inventory_state entity_id -> shop inventory (`inventory_state` joined on
    /// `barter_stall_state`). Sync phase only (cleared by clear_live_caches):
    /// it builds the combined `shop_inventory` snapshot; live-phase changes are
    /// passed through as half-updates and merged by the relay module.
    pub shop_inventories: HashMap<u64, ShopInventory>,
    /// trade_order_state entity_id -> player-set order (sync phase only; cleared
    /// by clear_live_caches). Traveler orders are never stored. Live-phase
    /// changes are emitted as upserts/deletes directly from the update batch.
    pub trade_orders: HashMap<u64, TradeOrderData>,
    /// Valid `barter_stall_inventory_event` rows seen during sync (cleared by
    /// clear_live_caches). The upstream table starts empty, so these are only
    /// sales that happened while subscriptions were still being applied; they are
    /// flushed to the relay with the snapshot so none are lost.
    pub trade_events: Vec<TradeEventRow>,
    /// This region's projected `region` row, folded from the three upstream
    /// region tables. Maintained in both phases and intentionally survives
    /// clear_live_caches: there are only ~25 regions world-wide and the data is
    /// near-static, so holding the whole row lets a change to any one of those
    /// tables be emitted as one complete row.
    pub region_info: RegionRow,
    /// Set when `region_info` changed and the new row has not been emitted yet.
    pub region_info_dirty: bool,
}

/// The subset of `claim_member_state` fields mirrored into the relay.
/// `user_name` is deliberately excluded — player names already live in the
/// relay `player_state` table, which is the source of truth in prism.
/// (BitCraft has to mirror renames across multiple tables instead.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimMemberData {
    pub claim_entity_id: u64,
    pub player_entity_id: u64,
    pub build: bool,
    pub inventory: bool,
    pub officer: bool,
    pub co_owner: bool,
}

impl ClaimMemberData {
    pub fn from_row(row: &upstream_bindings::region::ClaimMemberState) -> Self {
        Self {
            claim_entity_id: row.claim_entity_id,
            player_entity_id: row.player_entity_id,
            build: row.build_permission,
            inventory: row.inventory_permission,
            officer: row.officer_permission,
            co_owner: row.co_owner_permission,
        }
    }

    /// `owner` is not an upstream permission — it is derived from
    /// `claim_state.owner_player_entity_id` and passed in by the caller.
    pub fn into_row(self, entity_id: u64, region_id: u8, owner: bool) -> ClaimMemberRow {
        ClaimMemberRow {
            entity_id,
            region_id,
            claim_entity_id: self.claim_entity_id,
            player_entity_id: self.player_entity_id,
            build: self.build,
            inventory: self.inventory,
            officer: self.officer,
            co_owner: self.co_owner,
            owner,
        }
    }
}

/// A shop's claim (0 when none), nickname (empty when unnamed), owning player
/// (`deployable_state_v2.owner_id`; 0 for buildings) and kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShopMeta {
    pub claim_entity_id: u64,
    pub nickname: String,
    pub owner_id: u64,
    pub kind: ShopKind,
}

/// One `inventory_state` row owned by a shop (sync-phase cache).
#[derive(Clone, Debug)]
pub struct ShopInventory {
    pub owner_entity_id: u64,
    pub inventory_index: i32,
    pub cargo_index: i32,
    pub pockets: Vec<PocketRow>,
}

impl ShopInventory {
    pub fn from_row(row: &InventoryState) -> Self {
        Self {
            owner_entity_id: row.owner_entity_id,
            inventory_index: row.inventory_index,
            cargo_index: row.cargo_index,
            pockets: row.pockets.iter().map(pocket_row).collect(),
        }
    }
}

pub fn pocket_row(p: &Pocket) -> PocketRow {
    PocketRow {
        volume: p.volume,
        contents: p.contents.as_ref().map(item_stack_row),
        locked: p.locked,
    }
}

/// The halves of a shop's combined inventory that one upstream inventory
/// backs, with its pockets for each. Pockets before `cargo_index` are items,
/// the rest cargo. A barter stall's item inventory has `cargo_index == len`
/// (items only) and its cargo inventory `cargo_index == 0` (cargo only); a
/// trader stand's single inventory has both. Only halves the inventory
/// actually has are returned, so one never clobbers the other's.
pub fn inventory_halves(
    cargo_index: i32,
    pockets: &[PocketRow],
) -> impl Iterator<Item = (InventoryPartKind, &[PocketRow])> {
    let split = (cargo_index.max(0) as usize).min(pockets.len());
    let (items, cargo) = pockets.split_at(split);
    [
        (InventoryPartKind::Items, items),
        (InventoryPartKind::Cargo, cargo),
    ]
    .into_iter()
    .filter(|(_, half)| !half.is_empty())
}

/// The subset of a player-set `trade_order_state` row mirrored into the relay.
/// The shop's claim is joined in at emit time, so it is not stored.
#[derive(Clone, Debug)]
pub struct TradeOrderData {
    pub shop_entity_id: u64,
    pub remaining_stock: i32,
    pub offer_items: Vec<ItemStackRow>,
    pub required_items: Vec<ItemStackRow>,
}

impl TradeOrderData {
    /// `None` for traveler (NPC) orders, which the relay does not mirror.
    pub fn from_row(row: &TradeOrderState) -> Option<Self> {
        if row.traveler_trade_order_id.is_some() {
            return None;
        }
        Some(Self {
            shop_entity_id: row.shop_entity_id,
            remaining_stock: row.remaining_stock,
            offer_items: row.offer_items.iter().map(item_stack_row).collect(),
            required_items: row.required_items.iter().map(item_stack_row).collect(),
        })
    }

    pub fn into_row(
        self,
        id: u64,
        region_id: u8,
        claim_entity_id: u64,
        active: bool,
    ) -> TradeOrderRow {
        TradeOrderRow {
            id,
            shop_entity_id: self.shop_entity_id,
            remaining_stock: self.remaining_stock,
            offer_items: self.offer_items,
            required_items: self.required_items,
            claim_entity_id,
            region_id,
            active,
        }
    }
}

pub fn item_stack_row(item: &ItemStack) -> ItemStackRow {
    ItemStackRow {
        item_id: item.item_id,
        quantity: item.quantity,
        kind: match item.item_type {
            ItemType::Item => ItemKind::Item,
            ItemType::Cargo => ItemKind::Cargo,
        },
        durability: item.durability,
    }
}

impl TradeEventRow {
    /// Projects an upstream event row, dropping anything that is not a sale
    /// against a known trade order with a trade amount. The server currently
    /// emits nothing else, but that may change.
    pub fn from_upstream(region_id: u8, row: &BarterStallInventoryEvent) -> Option<Self> {
        if !matches!(row.reason, BarterStallInventoryChangeReason::Sale) {
            return None;
        }
        Some(Self {
            region_id,
            shop_entity_id: row.shop_entity_id,
            claim_entity_id: row.claim_entity_id,
            actor_entity_id: row.actor_entity_id,
            trade_order_entity_id: row.trade_order_entity_id?,
            trade_amount: row.trade_amount?,
            added_items: row.added_items.iter().map(item_stack_row).collect(),
            removed_items: row.removed_items.iter().map(item_stack_row).collect(),
            treasury_coins_added: row.treasury_coins_added,
            treasury_coins_removed: row.treasury_coins_removed,
            timestamp_micros: row.timestamp.to_micros_since_unix_epoch(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EntityLocation {
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
}

/// Sync-phase cache of the herd fields needed to build a projected relay row:
/// `enemy_ai_params_desc_id` resolves to `enemy_type` via `enemy_ai_type_map`,
/// `crumb_trail_entity_id` (> 0 means this herd is attached to another and
/// should be ignored, per game semantics).
#[derive(Clone, Copy, Debug)]
pub struct HerdSyncEntry {
    pub enemy_ai_params_desc_id: i32,
    pub crumb_trail_entity_id: u64,
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

    /// Collect all known overworld, non-crumb-trailed herds with a resolvable
    /// enemy type as relay rows for a bulk replace.
    pub fn snapshot_herds(&self, region_id: u8) -> Vec<HerdRow> {
        self.herd_kind
            .iter()
            .filter_map(|(&eid, entry)| {
                if entry.crumb_trail_entity_id > 0 || entry.enemy_ai_params_desc_id == 0 {
                    return None;
                }
                let enemy_type = *self.enemy_ai_type_map.get(&entry.enemy_ai_params_desc_id)?;
                if enemy_type == 0 {
                    return None;
                }
                let loc = self.last_location.get(&eid)?;
                if loc.dimension != OVERWORLD_DIM {
                    return None;
                }
                Some(HerdRow {
                    entity_id: eid,
                    enemy_type,
                    enemy_params_ai_desc_id: entry.enemy_ai_params_desc_id,
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

    /// Claim/nickname/owner/kind for a shop, which is either a deployable or a
    /// building. Unknown shops resolve to no claim, name or owner, and
    /// `ShopKind::Unknown`.
    pub fn shop_meta(&self, shop_entity_id: u64) -> ShopMeta {
        if let Some(info) = self.deployable_shops.get(&shop_entity_id) {
            return info.clone();
        }
        ShopMeta {
            claim_entity_id: self
                .building_claim_map
                .get(&shop_entity_id)
                .copied()
                .unwrap_or(0),
            nickname: self
                .building_nicknames
                .get(&shop_entity_id)
                .cloned()
                .unwrap_or_default(),
            owner_id: 0,
            kind: self
                .building_desc_ids
                .get(&shop_entity_id)
                .map_or(ShopKind::Unknown, |&id| ShopKind::BarterStall(id)),
        }
    }

    /// Collect all known player-set trade orders, with shop claims and the
    /// active flag joined, as relay rows for a bulk replace.
    pub fn snapshot_trade_orders(&self, region_id: u8) -> Vec<TradeOrderRow> {
        self.trade_orders
            .iter()
            .map(|(&id, order)| {
                let shop = order.shop_entity_id;
                let claim = self.shop_meta(shop).claim_entity_id;
                let active = self.barter_stalls.contains(&shop);
                order.clone().into_row(id, region_id, claim, active)
            })
            .collect()
    }

    /// The relay `shop_info` row for a shop, or `None` if it is not in the
    /// world (no `barter_stall_state` row).
    pub fn shop_info_row(&self, region_id: u8, shop_entity_id: u64) -> Option<ShopInfoRow> {
        if !self.barter_stalls.contains(&shop_entity_id) {
            return None;
        }
        let meta = self.shop_meta(shop_entity_id);
        Some(ShopInfoRow {
            shop_entity_id,
            region_id,
            claim_entity_id: meta.claim_entity_id,
            shop_name: meta.nickname,
            player_owner_id: meta.owner_id,
            shop_type: meta.kind,
        })
    }

    /// Every shop in the world, as relay rows for a bulk replace.
    pub fn snapshot_shop_info(&self, region_id: u8) -> Vec<ShopInfoRow> {
        self.barter_stalls
            .iter()
            .filter_map(|&shop| self.shop_info_row(region_id, shop))
            .collect()
    }

    /// Every shop's inventories merged into one row each (item halves, then
    /// cargo halves, each in `inventory_index` order), for a bulk replace.
    pub fn snapshot_shop_inventories(&self, region_id: u8) -> Vec<ShopInventoryRow> {
        let mut by_shop: HashMap<u64, Vec<(i32, u64, &ShopInventory)>> = HashMap::new();
        for (&id, inv) in &self.shop_inventories {
            by_shop
                .entry(inv.owner_entity_id)
                .or_default()
                .push((inv.inventory_index, id, inv));
        }
        by_shop
            .into_iter()
            .filter_map(|(shop_entity_id, mut invs)| {
                invs.sort_unstable_by_key(|&(index, id, _)| (index, id));
                let mut items = Vec::new();
                let mut cargo = Vec::new();
                for (_, _, inv) in invs {
                    for (part, half) in inventory_halves(inv.cargo_index, &inv.pockets) {
                        match part {
                            InventoryPartKind::Items => items.extend_from_slice(half),
                            InventoryPartKind::Cargo => cargo.extend_from_slice(half),
                        }
                    }
                }
                if items.is_empty() && cargo.is_empty() {
                    return None;
                }
                let cargo_index = items.len() as u32;
                items.extend(cargo);
                Some(ShopInventoryRow {
                    shop_entity_id,
                    region_id,
                    pockets: items,
                    cargo_index,
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
                name: self.claim_names.get(&eid).cloned().unwrap_or_default(),
                bank: self.claim_banks.contains(&eid),
                marketplace: self.claim_marketplaces.contains(&eid),
                waystone: self.claim_waystones.contains(&eid),
                research: self.claim_research.get(&eid).cloned().unwrap_or_default(),
            })
            .collect()
    }

    /// Collect all known claim memberships as relay rows for a bulk replace.
    pub fn snapshot_claim_members(&self, region_id: u8) -> Vec<ClaimMemberRow> {
        self.claim_members
            .iter()
            .map(|(&eid, data)| {
                let owner =
                    self.claim_owners.get(&data.claim_entity_id) == Some(&data.player_entity_id);
                data.into_row(eid, region_id, owner)
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
        self.herd_kind = HashMap::default();
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
        // claim_names / claim_research / claim_banks / claim_marketplaces /
        // claim_waystones are only needed to build the initial coherent
        // ClaimInfo snapshot; live-phase changes are emitted as targeted
        // field updates derived directly from update batches.
        self.claim_names = HashMap::default();
        self.claim_research = HashMap::default();
        self.claim_banks = HashSet::default();
        self.claim_marketplaces = HashSet::default();
        self.claim_waystones = HashSet::default();
        // claim_members is only needed to build the initial coherent
        // claim_member snapshot; live-phase changes are emitted as upserts /
        // deletes derived directly from update batches.
        self.claim_members = HashMap::default();
        // trade_orders / trade_events / shop_inventories are only needed for the
        // initial snapshot; live-phase changes are emitted straight from update
        // batches (inventories as half-updates the relay module merges).
        self.trade_orders = HashMap::default();
        self.trade_events = Vec::default();
        self.shop_inventories = HashMap::default();

        // building_nicknames and deployable_shops are retained: the live phase
        // joins them onto new trade orders / shop_info rows and diffs them to
        // spot shop changes. barter_stalls is likewise retained: it decides the
        // active flag of new orders and which shops have a shop_info row.
        //
        // resource_desc_tags is retained: the live phase reads it to
        // decide which newly-inserted resources warrant a growth-timer sub.
        //
        // enemy_ai_type_map is likewise retained: the live phase needs it to
        // resolve newly-spawned herds (see emit_deltas).
        //
        // claim_owners and region_info are likewise retained: both are small
        // and both are read in the live phase (to derive `ClaimMember::owner`
        // and to emit whole `region` rows respectively).
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pocket(item_id: i32, kind: ItemKind) -> PocketRow {
        PocketRow {
            volume: 6000,
            contents: Some(ItemStackRow {
                item_id,
                quantity: 1,
                kind,
                durability: None,
            }),
            locked: false,
        }
    }

    fn empty() -> PocketRow {
        PocketRow {
            volume: 6000,
            contents: None,
            locked: false,
        }
    }

    fn inv(owner: u64, index: i32, cargo_index: i32, pockets: Vec<PocketRow>) -> ShopInventory {
        ShopInventory {
            owner_entity_id: owner,
            inventory_index: index,
            cargo_index,
            pockets,
        }
    }

    #[test]
    fn halves_follow_cargo_index() {
        let pockets = vec![
            pocket(1, ItemKind::Item),
            empty(),
            pocket(9, ItemKind::Cargo),
        ];
        // Trader stand: both halves.
        let halves: Vec<_> = inventory_halves(2, &pockets).collect();
        assert_eq!(halves.len(), 2);
        assert_eq!(halves[0], (InventoryPartKind::Items, &pockets[..2]));
        assert_eq!(halves[1], (InventoryPartKind::Cargo, &pockets[2..]));
        // Stall item inventory (cargo_index == len): items only.
        let halves: Vec<_> = inventory_halves(3, &pockets).collect();
        assert_eq!(halves, vec![(InventoryPartKind::Items, &pockets[..])]);
        // Stall cargo inventory (cargo_index == 0): cargo only.
        let halves: Vec<_> = inventory_halves(0, &pockets).collect();
        assert_eq!(halves, vec![(InventoryPartKind::Cargo, &pockets[..])]);
        // Out-of-range cutoffs clamp rather than panic.
        assert_eq!(inventory_halves(99, &pockets).count(), 1);
        assert_eq!(inventory_halves(-1, &pockets).count(), 1);
        assert_eq!(inventory_halves(0, &[]).count(), 0);
    }

    #[test]
    fn snapshot_merges_stall_inventories() {
        let mut region = RegionJoinState::default();
        let items = vec![pocket(1, ItemKind::Item), empty()];
        let cargo = vec![pocket(9, ItemKind::Cargo)];
        let stand = vec![pocket(2, ItemKind::Item), pocket(8, ItemKind::Cargo)];
        // Stall 10: cargo inventory listed first to check ordering.
        region
            .shop_inventories
            .insert(101, inv(10, 1, 0, cargo.clone()));
        region
            .shop_inventories
            .insert(100, inv(10, 0, 2, items.clone()));
        // Stand 20: one mixed inventory.
        region
            .shop_inventories
            .insert(200, inv(20, 0, 1, stand.clone()));

        let mut rows = region.snapshot_shop_inventories(7);
        rows.sort_by_key(|r| r.shop_entity_id);
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].shop_entity_id, 10);
        assert_eq!(rows[0].region_id, 7);
        assert_eq!(rows[0].cargo_index, 2);
        assert_eq!(rows[0].pockets, [items, cargo].concat());

        assert_eq!(rows[1].shop_entity_id, 20);
        assert_eq!(rows[1].cargo_index, 1);
        assert_eq!(rows[1].pockets, stand);
    }

    #[test]
    fn orders_are_active_only_in_world_shops() {
        let mut region = RegionJoinState::default();
        region.barter_stalls.insert(10);
        let order = TradeOrderData {
            shop_entity_id: 10,
            remaining_stock: 1,
            offer_items: vec![],
            required_items: vec![],
        };
        region.trade_orders.insert(1, order.clone());
        region.trade_orders.insert(
            2,
            TradeOrderData {
                shop_entity_id: 20,
                ..order
            },
        );
        let mut rows = region.snapshot_trade_orders(7);
        rows.sort_by_key(|r| r.id);
        assert!(rows[0].active);
        assert!(!rows[1].active);
        // Only shops in the world get shop_info rows.
        assert!(region.shop_info_row(7, 10).is_some());
        assert!(region.shop_info_row(7, 20).is_none());
    }
}
