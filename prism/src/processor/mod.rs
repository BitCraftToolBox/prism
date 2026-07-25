//! Processor — consumes the unified [`RegionUpdate`] stream, joins related
//! tables to produce sink-shaped rows, applies the dim==1 filter, and fans
//! out to the relay and history sinks via bounded channels.
//!
//! State per region (resource_id ↔ entity_id, enemy_type ↔ entity_id, etc.)
//! is held in memory so location updates that arrive without their owning
//! row can still be resolved — same shape as nodeindex's `consume` task.

pub mod join;
pub mod pipeline;

use std::sync::Arc;

use anyhow::Result;
use log::{debug, warn};
use metrics::histogram;
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
};

use crate::config::{Config, PipelinesConfig};
use crate::history::{HistoryMsg, history_capacity, history_enabled};
use crate::relay::{RelayMsg, relay_capacity};
use crate::shutdown::SharedShutdown;
use crate::upstream::{GrowthTimerSubRequest, RegionUpdate};

use join::JoinState;

pub struct ProcessorSinks {
    pub relay_rx: Receiver<RelayMsg>,
    /// `None` when history is disabled (no database URL configured).
    pub history_rx: Option<Receiver<HistoryMsg>>,
}

pub struct ProcessorHandle {
    pub relay_tx: Sender<RelayMsg>,
    /// `None` when history is disabled; the pipeline drops history messages.
    pub history_tx: Option<Sender<HistoryMsg>>,
    /// Which pipelines this instance is responsible for — gates which tables
    /// the initial sync->live bulk replace is allowed to overwrite.
    pub pipelines: PipelinesConfig,
    /// Back-channel to the upstream connections: requests targeted
    /// `resource_growth_timer` subscriptions for newly-inserted tagged
    /// resources. Demultiplexed to the right region by `region_id`.
    pub growth_sub_tx: UnboundedSender<GrowthTimerSubRequest>,
}

/// Build the bounded sink channels plus the unbounded growth-timer
/// subscription back-channel. Returns (handle for processor task, receivers
/// for the sinks, receiver the upstream demux drains).
pub fn channels(
    config: &Config,
) -> (
    ProcessorHandle,
    ProcessorSinks,
    UnboundedReceiver<GrowthTimerSubRequest>,
) {
    let (relay_tx, relay_rx) = channel(relay_capacity(config));
    let (history_tx, history_rx) = if history_enabled(config) {
        let (tx, rx) = channel(history_capacity(config));
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let (growth_sub_tx, growth_sub_rx) = unbounded_channel();
    (
        ProcessorHandle {
            relay_tx,
            history_tx,
            pipelines: config.pipelines.clone(),
            growth_sub_tx,
        },
        ProcessorSinks {
            relay_rx,
            history_rx,
        },
        growth_sub_rx,
    )
}

/// Run the processor loop until the upstream channel closes (i.e. all
/// upstream tasks have exited).
pub async fn run(
    _config: Arc<Config>,
    mut rx: UnboundedReceiver<RegionUpdate>,
    handle: ProcessorHandle,
    _shutdown: SharedShutdown,
) -> Result<()> {
    let mut state = JoinState::new();
    while let Some(msg) = rx.recv().await {
        let t = std::time::Instant::now();
        if let Err(e) = pipeline::handle(&mut state, msg, &handle).await {
            warn!("processor error: {e:?}");
        }
        histogram!("prism_processor_latency_seconds").record(t.elapsed().as_secs_f64());
    }
    debug!("processor exiting (upstream channel closed)");
    Ok(())
}
