//! Prism relay SpacetimeDB module.
//!
//! Stores the current state (resources, enemies, players) for each BitCraft
//! region in a schema optimized for client-app subscriptions: indexed by
//! resource / enemy / region id rather than chunk index. Only the configured
//! relay identity (set once via [`init_relay`]) may write.

use spacetimedb::http::{Body, HandlerContext, Request, Response, Router};
use spacetimedb::{Identity, ReducerContext, Table, reducer, table};
// ---------- tables ----------

#[table(accessor = resource_location, public,
    index(accessor = by_resource, btree(columns = [resource_id])),
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_resource_and_region, btree(columns = [resource_id, region_id])))]
pub struct ResourceLocation {
    #[primary_key]
    pub entity_id: u64,
    pub resource_id: i32,
    pub region_id: u8,
    pub x: i32,
    pub z: i32,
}

#[table(accessor = enemy_location, public,
    index(accessor = by_enemy_type, btree(columns = [enemy_type])),
    index(accessor = by_region, btree(columns = [region_id])),
    index(accessor = by_enemy_type_and_region, btree(columns = [enemy_type, region_id])))]
pub struct EnemyLocation {
    #[primary_key]
    pub entity_id: u64,
    pub enemy_type: i32,
    pub x: i32,
    pub z: i32,
    pub region_id: u8,
}

#[table(accessor = player_location, public,
    index(accessor = by_region, btree(columns = [region_id])))]
pub struct PlayerLocation {
    #[primary_key]
    pub entity_id: u64,
    pub x: i32,
    pub z: i32,
    pub region_id: u8,
}

#[table(accessor = player_state, public,
    index(accessor = by_region, btree(columns = [region_id])))]
pub struct PlayerState {
    #[primary_key]
    pub entity_id: u64,
    pub region_id: u8,
    pub online: bool,
    pub name: String,
}

/// Single-row config table holding the identity of the authorized relay client.
#[table(accessor = relay_config)]
pub struct RelayConfig {
    #[primary_key]
    pub id: u8, // always 0
    pub identity: Identity,
}

// ---------- helpers ----------

fn ensure_relay(ctx: &ReducerContext) -> Result<(), String> {
    match ctx.db.relay_config().id().find(0) {
        Some(cfg) if cfg.identity == ctx.sender() => Ok(()),
        Some(_) => Err("caller is not the configured relay identity".into()),
        None => Err("relay identity is not yet configured; call init_relay first".into()),
    }
}

// ---------- reducers ----------

/// First-call-wins initialization: stores the caller's identity as the relay.
/// After this any other reducer rejects calls from other identities.
#[reducer]
pub fn init_relay(ctx: &ReducerContext) -> Result<(), String> {
    if let Some(cfg) = ctx.db.relay_config().id().find(0) {
        if cfg.identity == ctx.sender() {
            return Ok(());
        }
        return Err("relay identity already configured".into());
    }
    ctx.db.relay_config().insert(RelayConfig {
        id: 0,
        identity: ctx.sender(),
    });
    spacetimedb::log::debug!("relay: initialized with identity {:?}", ctx.sender());
    Ok(())
}

// --- resources ---

#[reducer]
pub fn bulk_replace_resources(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<ResourceLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_resources for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.resource_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.resource_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_resources(ctx: &ReducerContext, rows: Vec<ResourceLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.resource_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_resources(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.resource_location().entity_id().delete(id);
    }
    Ok(())
}

// --- enemies ---

#[reducer]
pub fn bulk_replace_enemies(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<EnemyLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_enemies for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.enemy_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.enemy_location().insert(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_enemies(ctx: &ReducerContext, rows: Vec<EnemyLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.enemy_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_enemies(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.enemy_location().entity_id().delete(id);
    }
    Ok(())
}

// --- players ---

#[reducer]
pub fn bulk_replace_players(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<PlayerLocation>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_players for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.player_location().by_region().delete(region_id);
    for row in rows {
        ctx.db.player_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_players(ctx: &ReducerContext, rows: Vec<PlayerLocation>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.player_location().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_players(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.player_location().entity_id().delete(id);
    }
    Ok(())
}

// --- player states ---

#[reducer]
pub fn bulk_replace_player_states(
    ctx: &ReducerContext,
    region_id: u8,
    rows: Vec<PlayerState>,
    total: u32,
) -> Result<(), String> {
    ensure_relay(ctx)?;
    spacetimedb::log::info!(
        "relay: processing bulk_replace_player_states for region {:?}: {:?}/{:?} rows",
        region_id,
        rows.len(),
        total
    );
    ctx.db.player_state().by_region().delete(region_id);
    for row in rows {
        ctx.db.player_state().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn upsert_player_states(ctx: &ReducerContext, rows: Vec<PlayerState>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for row in rows {
        ctx.db.player_state().entity_id().insert_or_update(row);
    }
    Ok(())
}

#[reducer]
pub fn delete_player_states(ctx: &ReducerContext, entity_ids: Vec<u64>) -> Result<(), String> {
    ensure_relay(ctx)?;
    for id in entity_ids {
        ctx.db.player_state().entity_id().delete(id);
    }
    Ok(())
}

// --- http ---

#[spacetimedb::http::handler]
fn players(ctx: &mut HandlerContext, request: Request) -> Response {
    let url = url::Url::parse(request.uri().to_string().as_str());
    let query = url.ok().and_then(|u| {
        u.query_pairs()
            .find(|(k, _)| k == "q")
            .map(|(_, v)| v.into_owned())
    });

    let body = if let Some(q) = query {
        let q_lower = q.to_lowercase();
        ctx.with_tx(|tx| {
            let mut rows: Vec<_> = tx
                .db
                .player_state()
                .iter()
                .filter(|p| p.name.to_lowercase().contains(&q_lower))
                .collect();

            rows.sort_by(|a, b| {
                let a_lower = a.name.to_lowercase();
                let b_lower = b.name.to_lowercase();

                let a_exact = a_lower == q_lower;
                let b_exact = b_lower == q_lower;

                if a_exact != b_exact {
                    return if a_exact {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }

                let a_starts = a_lower.starts_with(&q_lower);
                let b_starts = b_lower.starts_with(&q_lower);

                if a_starts != b_starts {
                    return if a_starts {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }

                std::cmp::Ordering::Equal
            });

            let results: Vec<_> = rows
                .into_iter()
                .map(|p| serde_json::json!({
                    "entityId": p.entity_id.to_string(),
                    "username": p.name,
                    "signedIn": p.online
                }))
                .collect();

            serde_json::to_vec(&results).ok()
        })
    } else {
        Some(vec![])
    };

    if let Some(body) = body {
        Response::builder().status(200).header("Content-Type", "application/json").body(Body::from_bytes(body)).unwrap()
    } else {
        Response::builder().status(404).body(Body::empty()).unwrap()
    }
}

#[spacetimedb::http::router]
fn router() -> Router {
    Router::new().get("/players", players)
}
