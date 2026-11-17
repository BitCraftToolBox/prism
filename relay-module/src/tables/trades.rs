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

/// A player-set trade order (`trade_order_state`) on a barter stall, with the
/// shop's claim and name joined in prism. Traveler (NPC) orders are filtered
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
    /// Building / deployable nickname; empty when unnamed.
    pub shop_name: String,
}

/// A completed sale, mirrored from the upstream `barter_stall_inventory_event`
/// event table. Upstream rows are ephemeral; here they persist for 24h in
/// `trade_event` (see `remove_trade_event`), after which they are moved to
/// `trade_event_history` (same schema and ids).
#[table(accessor = trade_event, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_shop, btree(columns = [shop_entity_id])),
)]
#[table(accessor = trade_event_history, public,
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_shop, btree(columns = [shop_entity_id])),
)]
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
