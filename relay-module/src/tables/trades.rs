use spacetimedb::{SpacetimeType, Timestamp, table};

/// Copy of upstream `ItemType`.
#[derive(SpacetimeType, Clone, Copy, PartialEq, Eq, Debug)]
pub enum ItemType {
    Item,
    Cargo,
}

/// Copy of upstream `ItemStack`.
#[derive(SpacetimeType, Clone, PartialEq, Debug)]
pub struct ItemStack {
    pub item_id: i32,
    pub quantity: i32,
    pub item_type: ItemType,
    pub durability: Option<i32>,
}

/// Copy of upstream `BarterStallInventoryChangeReason`.
#[derive(SpacetimeType, Clone, Copy, PartialEq, Eq, Debug)]
pub enum BarterStallInventoryChangeReason {
    Sale,
}

/// Copy of upstream `Pocket` (one inventory slot).
#[derive(SpacetimeType, Clone, PartialEq, Debug)]
pub struct Pocket {
    pub volume: i32,
    pub contents: Option<ItemStack>,
    pub locked: bool,
}

/// What kind of shop a `shop_info` row is. The payload is the upstream
/// description id (`building_description_id` / `deployable_description_id`);
/// `Unknown` carries a meaningless 0 as padding.
#[derive(SpacetimeType, Clone, Copy, PartialEq, Eq, Debug)]
pub enum ShopType {
    BarterStall(i32),
    TraderStand(i32),
    Unknown(i32),
}

/// A player-set trade order (`trade_order_state`) on a barter stall, with the
/// shop's claim joined in prism. Traveler (NPC) orders are filtered
/// out before they get here. Replaced wholesale per region on each
/// sync→live transition, then upserted/deleted incrementally.
#[table(accessor = trade_order, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_shop, btree(columns = [shop_entity_id])),
)]
pub struct TradeOrder {
    /// Upstream `trade_order_state.entity_id`.
    #[primary_key]
    #[index(hash)]
    pub id: u64,
    /// Either a `building_state` or a `deployable_state_v2` entity id.
    pub shop_entity_id: u64,
    pub remaining_stock: i32,
    pub offer_items: Vec<ItemStack>,
    pub required_items: Vec<ItemStack>,
    /// Claim the shop sits in; 0 when none.
    pub claim_entity_id: u64,
    pub region_id: u8,
    /// Unused (always empty); the shop's name lives on `shop_info.shop_name`.
    pub shop_name: String,
    /// Whether the shop is currently in the world (has a `shop_info` row).
    /// False while a trader stand is stowed, or for orders whose shop no longer
    /// exists; such orders are kept upstream but cannot be traded with.
    #[default(true)]
    pub active: bool,
}

/// A shop in the world: a barter stall building or a placed trader stand,
/// i.e. one upstream `barter_stall_state` row (stowed stands have none). Exists
/// whether or not the shop has orders. Replaced wholesale per region on each
/// sync→live transition, then upserted/deleted incrementally.
#[table(accessor = shop_info, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_claim, btree(columns = [claim_entity_id])),
    index(accessor = by_owner, btree(columns = [player_owner_id])),
)]
pub struct ShopInfo {
    /// Either a `building_state` or a `deployable_state_v2` entity id.
    #[primary_key]
    pub shop_entity_id: u64,
    pub region_id: u8,
    /// Claim the shop sits in; 0 when none.
    pub claim_entity_id: u64,
    /// Building / deployable nickname; empty when unnamed.
    pub shop_name: String,
    /// `deployable_state_v2.owner_id` of the shop; 0 for buildings (or unknown shops).
    pub player_owner_id: u64,
    pub shop_type: ShopType,
}

/// A shop's storage as one inventory with the usual cargo cutoff: pockets
/// before `cargo_index` hold items, the rest hold cargo. Trader stands have a
/// single upstream inventory in this shape already; barter stalls have an
/// item-only and a cargo-only inventory, merged here (see
/// `update_shop_inventories`). The row exists while either half has pockets.
#[table(accessor = shop_inventory, public,
    index(accessor = by_region, btree(columns = [region_id])),
)]
pub struct ShopInventory {
    #[primary_key]
    pub shop_entity_id: u64,
    pub region_id: u8,
    pub pockets: Vec<Pocket>,
    pub cargo_index: u32,
}

/// A completed sale, mirrored from the upstream `barter_stall_inventory_event`
/// event table. Upstream rows are ephemeral; here they persist for 24h in
/// `trade_event` (see `remove_trade_event`). Every event is also written to
/// `trade_event_history` (same schema and ids) at insert, so history holds the
/// full log.
#[table(accessor = trade_event, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_shop, btree(columns = [shop_entity_id])),
    index(accessor = by_claim, btree(columns = [claim_entity_id])),
)]
#[table(accessor = trade_event_history, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_shop, btree(columns = [shop_entity_id])),
    index(accessor = by_claim, btree(columns = [claim_entity_id])),
)]
#[derive(Clone)]
pub struct TradeEvent {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub region_id: u8,
    pub shop_entity_id: u64,
    pub claim_entity_id: u64,
    pub actor_entity_id: u64,
    pub reason: BarterStallInventoryChangeReason,
    pub trade_order_entity_id: Option<u64>,
    pub trade_amount: Option<i32>,
    pub added_items: Vec<ItemStack>,
    pub removed_items: Vec<ItemStack>,
    pub treasury_coins_added: i32,
    pub treasury_coins_removed: i32,
    pub timestamp: Timestamp,
}
