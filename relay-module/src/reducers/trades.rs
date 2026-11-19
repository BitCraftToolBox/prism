use crate::reducers::ensure_relay;
use crate::tables::trades::{
    Pocket, ShopInfo, ShopInventory, TradeEvent, TradeOrder, shop_info, shop_inventory,
    trade_event, trade_event_history, trade_order,
};
use spacetimedb::{ReducerContext, ScheduleAt, SpacetimeType, Table, TimeDuration, reducer, table};

#[table(accessor = expiring_trade_events, scheduled(remove_trade_event))]
pub struct ExpiringTradeEvent {
    #[primary_key]
    pub event_id: u64,
    pub scheduled_at: ScheduleAt,
}

/// New claim for every order sitting in `shop_entity_id`.
#[derive(SpacetimeType)]
pub struct ShopClaimUpdate {
    pub shop_entity_id: u64,
    pub claim_entity_id: u64,
}

/// Whether every order in `shop_entity_id` is tradeable (the shop is in the world).
#[derive(SpacetimeType)]
pub struct ShopActiveUpdate {
    pub shop_entity_id: u64,
    pub active: bool,
}

/// Which half of a `shop_inventory` row an update replaces.
#[derive(SpacetimeType, Clone, Copy, PartialEq, Eq, Debug)]
pub enum InventoryPart {
    /// Pockets before `cargo_index`.
    Items,
    /// Pockets from `cargo_index` on.
    Cargo,
}

/// Replace one half of a shop's inventory. An empty `pockets` clears that half
/// (the upstream inventory backing it went away).
#[derive(SpacetimeType)]
pub struct ShopInventoryUpdate {
    pub shop_entity_id: u64,
    pub region_id: u8,
    pub part: InventoryPart,
    pub pockets: Vec<Pocket>,
}

/// Replace the entire trade order set for a region in one transaction. Used
/// on the sync→live transition.
#[reducer]
pub fn bulk_replace_trade_orders(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<TradeOrder>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_trade_orders for region {:?}: orders={}",
        region_id,
        rows.len(),
    );
    ctx.db.trade_order().by_region().delete(region_id);
    for row in rows {
        ctx.db.trade_order().insert(row);
    }
    Ok(())
}

/// Live-phase: upsert orders that are new or whose stock/items changed.
#[reducer]
pub fn upsert_trade_orders(ctx: &ReducerContext, rows: Vec<TradeOrder>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.trade_order().id().insert_or_update(row);
    }
    Ok(())
}

/// Live-phase: orders removed upstream (canceled, shop destroyed, ...).
#[reducer]
pub fn delete_trade_orders(ctx: &ReducerContext, ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in ids {
        ctx.db.trade_order().id().delete(id);
    }
    Ok(())
}

/// Live-phase: a shop's claim changed. Updates every order in the shop; shops
/// with no orders are a no-op. (`shop_info` rows are re-sent whole instead.)
#[reducer]
pub fn set_shop_claims(ctx: &ReducerContext, updates: Vec<ShopClaimUpdate>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        for mut row in ctx.db.trade_order().by_shop().filter(update.shop_entity_id) {
            if row.claim_entity_id != update.claim_entity_id {
                row.claim_entity_id = update.claim_entity_id;
                ctx.db.trade_order().id().update(row);
            }
        }
    }
    Ok(())
}

/// Live-phase: shops placed / stowed / destroyed. Updates every order in the
/// shop; shops with no orders are a no-op.
#[reducer]
pub fn set_shop_active(ctx: &ReducerContext, updates: Vec<ShopActiveUpdate>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        for mut row in ctx.db.trade_order().by_shop().filter(update.shop_entity_id) {
            if row.active != update.active {
                row.active = update.active;
                ctx.db.trade_order().id().update(row);
            }
        }
    }
    Ok(())
}

/// Replace every shop inventory for a region in one transaction. Used on the
/// sync→live transition.
#[reducer]
pub fn bulk_replace_shop_inventories(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<ShopInventory>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_shop_inventories for region {:?}: shops={}",
        region_id,
        rows.len(),
    );
    ctx.db.shop_inventory().by_region().delete(region_id);
    for row in rows {
        ctx.db.shop_inventory().insert(row);
    }
    Ok(())
}

/// Live-phase: replace one half of a shop's inventory each. The other half is
/// kept, so a barter stall's item and cargo inventories update independently.
/// A row left with no pockets at all is deleted.
#[reducer]
pub fn update_shop_inventories(
    ctx: &ReducerContext,
    updates: Vec<ShopInventoryUpdate>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        let existing = ctx
            .db
            .shop_inventory()
            .shop_entity_id()
            .find(update.shop_entity_id);
        let (mut items, mut cargo): (Vec<Pocket>, Vec<Pocket>) = match existing {
            Some(row) => {
                let mut pockets = row.pockets;
                let split = (row.cargo_index as usize).min(pockets.len());
                let cargo = pockets.split_off(split);
                (pockets, cargo)
            }
            None => (Vec::new(), Vec::new()),
        };
        match update.part {
            InventoryPart::Items => items = update.pockets,
            InventoryPart::Cargo => cargo = update.pockets,
        }
        if items.is_empty() && cargo.is_empty() {
            ctx.db
                .shop_inventory()
                .shop_entity_id()
                .delete(update.shop_entity_id);
            continue;
        }
        let cargo_index = items.len() as u32;
        items.extend(cargo);
        ctx.db
            .shop_inventory()
            .shop_entity_id()
            .insert_or_update(ShopInventory {
                shop_entity_id: update.shop_entity_id,
                region_id: update.region_id,
                pockets: items,
                cargo_index,
            });
    }
    Ok(())
}

/// Replace the entire shop set for a region in one transaction. Used on the
/// sync→live transition.
#[reducer]
pub fn bulk_replace_shop_info(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<ShopInfo>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_shop_info for region {:?}: shops={}",
        region_id,
        rows.len(),
    );
    ctx.db.shop_info().by_region().delete(region_id);
    for row in rows {
        ctx.db.shop_info().insert(row);
    }
    Ok(())
}

/// Live-phase: shops placed/built, or whose metadata changed.
#[reducer]
pub fn upsert_shop_info(ctx: &ReducerContext, rows: Vec<ShopInfo>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.shop_info().shop_entity_id().insert_or_update(row);
    }
    Ok(())
}

/// Live-phase: shops stowed or destroyed.
#[reducer]
pub fn delete_shop_info(ctx: &ReducerContext, ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in ids {
        ctx.db.shop_info().shop_entity_id().delete(id);
    }
    Ok(())
}

/// Append completed sales. Event rows have no natural key, so `id` on the
/// passed rows is ignored (auto-inc) and each insert schedules its own
/// expiry 24h out.
#[reducer]
pub fn insert_trade_events(ctx: &ReducerContext, rows: Vec<TradeEvent>) -> Result<(), String> {
    ensure_relay(ctx)?;
    let after_24h = TimeDuration::from_micros(24 * 60 * 60 * 1_000_000);
    for row in rows {
        let inserted = ctx.db.trade_event().insert(row);
        // History is the full log: write it now (same id) rather than at expiry.
        ctx.db.trade_event_history().insert(inserted.clone());
        let scheduled_at: ScheduleAt = (ctx.timestamp + after_24h).into();
        ctx.db.expiring_trade_events().insert(ExpiringTradeEvent {
            event_id: inserted.id,
            scheduled_at,
        });
    }
    Ok(())
}

#[reducer]
fn remove_trade_event(
    ctx: &ReducerContext,
    expiring_event: ExpiringTradeEvent,
) -> Result<(), String> {
    // Events inserted before history moved to insert time were never written
    // there; backfill them now so they aren't lost when the row expires.
    if let Some(event) = ctx.db.trade_event().id().find(expiring_event.event_id) {
        if ctx.db.trade_event_history().id().find(event.id).is_none() {
            ctx.db.trade_event_history().insert(event);
        }
    }
    ctx.db.trade_event().id().delete(expiring_event.event_id);
    Ok(())
}
