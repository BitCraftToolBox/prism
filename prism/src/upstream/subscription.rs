//! Pipeline definitions — named bundles of upstream subscription queries.
//!
//! Each pipeline is subscribed sequentially via [`QueueSub`] (smaller
//! subscriptions are empirically more reliable than one giant one). Pipelines
//! are intentionally non-overlapping: when a future feature needs data that
//! some existing pipeline already subscribes to, it should consume the
//! existing pipeline's updates rather than declare its own subscription.

use std::sync::Arc;

use log::{error, info};
use upstream_bindings::region::{DbConnection, ErrorContext, SubscriptionEventContext};
use upstream_bindings::sdk::{DbContext, Error as SdkError};

use crate::config::PipelinesConfig;

/// All pipeline kinds. Order is the subscription order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pipeline {
    /// `resource_state` + `location_state` join. Feeds the relay
    /// `resource_location` table (dim == 1).
    Resources,
    /// `enemy_state` + `mobile_entity_state` join. Feeds the relay
    /// `enemy_location` table (dim == 1).
    Enemies,
    /// `player_state` + `mobile_entity_state` join. Feeds the relay
    /// `player_location` table (dim == 1) **and** the history sink
    /// (heatmap, dim == 1).
    Players,
}

impl Pipeline {
    fn queries(self) -> Vec<String> {
        match self {
            Pipeline::Resources => vec![
                "SELECT res.* FROM resource_state res \
                 JOIN location_state loc ON res.entity_id = loc.entity_id;".into(),
                "SELECT loc.* FROM location_state loc \
                 JOIN resource_state res ON loc.entity_id = res.entity_id;".into(),
            ],
            Pipeline::Enemies => vec![
                "SELECT * FROM enemy_state;".into(),
                "SELECT * FROM mobile_entity_state;".into(),
            ],
            Pipeline::Players => vec![
                "SELECT * FROM player_state;".into(),
                "SELECT * FROM mobile_entity_state;".into(),
            ],
        }
    }
}

pub fn enabled_pipelines(cfg: &PipelinesConfig) -> Vec<Pipeline> {
    let mut out = Vec::new();
    if cfg.resources { out.push(Pipeline::Resources); }
    if cfg.enemies   { out.push(Pipeline::Enemies); }
    if cfg.players   { out.push(Pipeline::Players); }
    out
}

/// Sequentially subscribe to each pipeline's queries on the given connection,
/// invoking `on_all_applied` exactly once after the *last* subscription is
/// applied. On any subscription error the connection is disconnected.
///
/// `region` is used only for log messages.
pub fn queue_subscribe<F>(
    ctx: &DbConnection,
    region: &str,
    pipelines: Vec<Pipeline>,
    on_all_applied: F,
) where
    F: FnOnce() + Send + 'static,
{
    let state = Arc::new(SubState {
        region: region.to_string(),
        pipelines,
        on_all_applied: std::sync::Mutex::new(Some(Box::new(on_all_applied))),
    });
    advance(ctx, state, 0);
}

struct SubState {
    region: String,
    pipelines: Vec<Pipeline>,
    on_all_applied: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

fn advance(ctx: &DbConnection, state: Arc<SubState>, idx: usize) {
    if idx >= state.pipelines.len() {
        if let Some(cb) = state.on_all_applied.lock().unwrap().take() {
            cb();
        }
        return;
    }
    let pipeline = state.pipelines[idx];
    let queries = pipeline.queries();
    info!(
        "[{}] [{}/{}] subscribing pipeline {:?}",
        state.region,
        idx + 1,
        state.pipelines.len(),
        pipeline
    );

    let state_for_applied = state.clone();
    let state_for_error = state.clone();

    ctx.subscription_builder()
        .on_error(move |ectx: &ErrorContext, e: SdkError| {
            error!(
                "[{}] subscription error in pipeline {:?}: {:?}",
                state_for_error.region,
                state_for_error.pipelines[idx], e
            );
            let _ = ectx.disconnect();
        })
        .on_applied(move |sub_ctx: &SubscriptionEventContext| {
            advance_ctx(sub_ctx, state_for_applied.clone(), idx + 1);
        })
        .subscribe(queries);
}

fn advance_ctx<C>(ctx: &C, state: Arc<SubState>, idx: usize)
where
    C: DbContext<
        DbView = <DbConnection as DbContext>::DbView,
        Reducers = <DbConnection as DbContext>::Reducers,
        SetReducerFlags = <DbConnection as DbContext>::SetReducerFlags,
        SubscriptionBuilder = <DbConnection as DbContext>::SubscriptionBuilder,
    >,
{
    if idx >= state.pipelines.len() {
        if let Some(cb) = state.on_all_applied.lock().unwrap().take() {
            cb();
        }
        return;
    }
    let pipeline = state.pipelines[idx];
    let queries = pipeline.queries();
    info!(
        "[{}] [{}/{}] subscribing pipeline {:?}",
        state.region,
        idx + 1,
        state.pipelines.len(),
        pipeline
    );

    let state_for_applied = state.clone();
    let state_for_error = state.clone();

    ctx.subscription_builder()
        .on_error(move |ectx: &ErrorContext, e: SdkError| {
            error!(
                "[{}] subscription error in pipeline {:?}: {:?}",
                state_for_error.region,
                state_for_error.pipelines[idx], e
            );
            let _ = ectx.disconnect();
        })
        .on_applied(move |sub_ctx: &SubscriptionEventContext| {
            advance_ctx(sub_ctx, state_for_applied.clone(), idx + 1);
        })
        .subscribe(queries);
}
