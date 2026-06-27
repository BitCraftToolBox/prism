use crate::reducers::ensure_relay;
use crate::tables::crafts::{
    CraftContribution, CraftMeta, CraftProgress, RecipeMeta, craft_contribution, craft_meta,
    craft_progress, recipe_meta,
};
use spacetimedb::{
    ReducerContext, ScheduleAt, SpacetimeType, Table, TimeDuration, Timestamp, reducer, table,
};

#[table(accessor = expiring_crafts, scheduled(remove_craft))]
pub struct ExpiringCraft {
    #[primary_key]
    pub craft_id: u64,
    pub scheduled_at: ScheduleAt,
}

#[derive(SpacetimeType)]
pub struct CraftUpdate {
    pub entity_id: u64,
    pub owner_entity_id: u64,
    pub claim_entity_id: u64,
    pub building_entity_id: u64,
    pub first_seen: Timestamp,
    pub recipe_id: i32,
    pub count: i32,
    pub region_id: u8,
    pub public: bool,
    pub progress: i32,
    pub last_seen: Timestamp,
}

#[derive(SpacetimeType)]
pub struct CraftContributionDelta {
    pub craft_id: u64,
    pub player_id: u64,
    pub progress_delta: i32,
    pub progress_total: i32,
    pub last_seen: Timestamp,
}

#[derive(SpacetimeType)]
pub struct CraftPublicUpdate {
    pub craft_id: u64,
    pub public: bool,
}

#[reducer]
pub fn upsert_crafts(ctx: &ReducerContext, rows: Vec<CraftUpdate>) -> Result<(), String> {
    ensure_relay(ctx)?;
    upsert_craft_rows(ctx, rows);
    Ok(())
}

#[reducer]
pub fn upsert_recipe_meta(ctx: &ReducerContext, rows: Vec<RecipeMeta>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.recipe_meta().id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_recipe_meta(ctx: &ReducerContext, recipe_ids: Vec<i32>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for recipe_id in recipe_ids {
        ctx.db.recipe_meta().id().delete(recipe_id);
    }
    Ok(())
}

#[reducer]
pub fn toggle_public(ctx: &ReducerContext, updates: Vec<CraftPublicUpdate>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for update in updates {
        if let Some(mut row) = ctx.db.craft_meta().entity_id().find(update.craft_id) {
            row.public = update.public;
            ctx.db.craft_meta().entity_id().update(row);
        }
    }
    Ok(())
}

#[reducer]
pub fn apply_craft_progress_deltas(
    ctx: &ReducerContext,
    deltas: Vec<CraftContributionDelta>,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    for delta in deltas {
        if delta.progress_delta == 0 {
            continue;
        }

        if let Some(row) = ctx
            .db
            .craft_contribution()
            .by_craft_and_player()
            .filter((delta.craft_id, delta.player_id)).next() {
            ctx.db.craft_contribution().id().update(CraftContribution {
                contribution: row.contribution + delta.progress_delta,
                ..row
            });
        } else {
            ctx.db.craft_contribution().insert(CraftContribution {
                id: 0,
                craft_id: delta.craft_id,
                player_id: delta.player_id,
                contribution: delta.progress_delta
            });
        }

        ctx.db
            .craft_progress()
            .entity_id()
            .insert_or_update(CraftProgress {
                entity_id: delta.craft_id,
                last_seen: delta.last_seen,
                progress: delta.progress_total,
            });
    }
    Ok(())
}

#[reducer]
pub fn schedule_craft_expiry(ctx: &ReducerContext, craft_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    let after_24h = TimeDuration::from_micros(24 * 60 * 60 * 1_000_000);
    for craft_id in craft_ids {
        let scheduled_at: ScheduleAt = (ctx.timestamp + after_24h).into();
        ctx.db
            .expiring_crafts()
            .craft_id()
            .insert_or_update(ExpiringCraft {
                craft_id,
                scheduled_at,
            });
    }
    Ok(())
}

#[reducer]
fn remove_craft(ctx: &ReducerContext, expiring_craft: ExpiringCraft) -> Result<(), String> {
    ensure_relay(ctx)?;
    let craft_id = expiring_craft.craft_id;
    ctx.db.craft_meta().entity_id().delete(craft_id);
    ctx.db.craft_progress().entity_id().delete(craft_id);
    ctx.db.craft_contribution().by_craft().delete(craft_id);
    Ok(())
}

fn upsert_craft_rows(ctx: &ReducerContext, rows: Vec<CraftUpdate>) {
    for row in rows {
        let first_seen = match ctx.db.craft_meta().entity_id().find(row.entity_id) {
            Some(existing) => existing.first_seen,
            None => row.first_seen,
        };
        ctx.db.craft_meta().entity_id().insert_or_update(CraftMeta {
            entity_id: row.entity_id,
            owner_entity_id: row.owner_entity_id,
            claim_entity_id: row.claim_entity_id,
            building_entity_id: row.building_entity_id,
            first_seen,
            recipe_id: row.recipe_id,
            count: row.count,
            region_id: row.region_id,
            public: row.public,
        });
        ctx.db
            .craft_progress()
            .entity_id()
            .insert_or_update(CraftProgress {
                entity_id: row.entity_id,
                last_seen: row.last_seen,
                progress: row.progress,
            });
    }
}
