use crate::reducers::ensure_relay;
use crate::tables::trades::{
    TradeEvent, TradeOrder, trade_event, trade_event_history, trade_order,
};
use spacetimedb::{ReducerContext, ScheduleAt, SpacetimeType, Table, TimeDuration, reducer, table};

#[table(accessor = expiring_trade_events, scheduled(remove_trade_event))]
pub struct ExpiringTradeEvent {
    #[primary_key]
    pub event_id: u64,
    pub scheduled_at: ScheduleAt,
}

/// New claim/nickname for every order sitting in `shop_entity_id`.
#[derive(SpacetimeType)]
pub struct ShopInfoUpdate {
    pub shop_entity_id: u64,
    pub claim_entity_id: u64,
    pub shop_name: String,
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

/// Live-phase: orders removed upstream (cancelled, shop destroyed, ...).
#[reducer]
pub fn delete_trade_orders(ctx: &ReducerContext, ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in ids {
        ctx.db.trade_order().id().delete(id);
    }
    Ok(())
}

/// Live-phase: a shop's claim or nickname changed. Updates every order in the
/// shop; shops with no orders are a no-op.
#[reducer]
pub fn set_shop_info(ctx: &ReducerContext, updates: Vec<ShopInfoUpdate>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        for mut row in ctx.db.trade_order().by_shop().filter(update.shop_entity_id) {
            if row.claim_entity_id != update.claim_entity_id || row.shop_name != update.shop_name {
                row.claim_entity_id = update.claim_entity_id;
                row.shop_name = update.shop_name.clone();
                ctx.db.trade_order().id().update(row);
            }
        }
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
    // Move the expired row into the history table instead of deleting it.
    if let Some(row) = ctx.db.trade_event().id().find(expiring_event.event_id) {
        ctx.db.trade_event().id().delete(expiring_event.event_id);
        ctx.db.trade_event_history().insert(row);
    }
    Ok(())
}
