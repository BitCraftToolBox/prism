use spacetimedb::{ReducerContext, Table, reducer};

use crate::tables::RelayConfig;
use crate::tables::relay_config;

pub mod enemies;
pub mod mobs;
pub mod players;
pub mod resources;

pub(crate) fn ensure_relay(ctx: &ReducerContext) -> Result<(), String> {
    match ctx.db.relay_config().id().find(0) {
        Some(cfg) if cfg.identity == ctx.sender() => Ok(()),
        Some(_) => Err("caller is not the configured relay identity".into()),
        None => Err("relay identity is not yet configured; call init_relay first".into()),
    }
}

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
