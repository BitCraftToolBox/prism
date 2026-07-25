//! Upstream BitCraft connector — one independently-managed connection per
//! configured region module.
//!
//! Each region task uses the [cacheless] fork of the SpacetimeDB SDK so that
//! row updates bypass the client cache and arrive on an `mpsc` channel. We
//! drain that per-region channel, tag each [`DbUpdate`] with the region id and
//! the current sync phase (Snapshot vs Delta), and forward to the shared
//! processor.
//!
//! [cacheless]: https://github.com/BitCraftToolBox/cacheless-rust-bindings

pub mod connection;
pub mod subscription;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, unbounded_channel};
use upstream_bindings::region::{DbUpdate, Reducer};

use crate::config::Config;
use crate::dumper::DumpMsg;
use crate::shutdown::SharedShutdown;
#[cfg(unix)]
use log::{error, info};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use upstream_bindings::sdk::Event;

/// A region update destined for the processor. Carries the originating region
/// id and the sync phase at the moment it was drained from the cacheless
/// channel.
pub struct RegionUpdate {
    pub region_id: u8,
    pub phase: Phase,
    pub update: DbUpdate,
    pub reducer: Event<Reducer>,
}

/// A request from the processor back to a region's live connection: subscribe
/// to `resource_growth_timer` for the given resource `entity_id`s (one query
/// per id, batched into a single subscription). Emitted when a newly-inserted
/// resource carries a tag listed in
/// [`crate::config::PipelinesConfig::growth_resource_tags`].
pub struct GrowthTimerSubRequest {
    pub region_id: u8,
    pub entity_ids: Vec<u64>,
}

/// Per-region sync phase. Stored as an `AtomicU8` shared between the
/// connection task and the channel-drain task so the latter can stamp each
/// raw update as it goes by.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Subscriptions are still being applied — updates are part of the
    /// initial snapshot and should accumulate, to be flushed as a single
    /// `ReplaceRegion` once `Live` is reached.
    Syncing = 0,
    /// All subscriptions are live — updates are incremental deltas.
    Live = 1,
}

impl Phase {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Phase::Live,
            _ => Phase::Syncing,
        }
    }
}

pub type SharedPhase = Arc<AtomicU8>;

pub fn store_phase(p: &SharedPhase, phase: Phase) {
    p.store(phase as u8, Ordering::SeqCst);
}

pub fn load_phase(p: &SharedPhase) -> Phase {
    Phase::from_u8(p.load(Ordering::SeqCst))
}

/// Spawn one connection task per configured region. All tasks share the
/// supplied processor channel and shutdown coordinator.
///
/// Returns once *all* region tasks have exited (either cleanly via shutdown
/// or via a fatal error).
pub async fn run_all(
    config: Arc<Config>,
    tx: tokio::sync::mpsc::UnboundedSender<RegionUpdate>,
    dump_tx: Sender<DumpMsg>,
    growth_sub_rx: UnboundedReceiver<GrowthTimerSubRequest>,
    shutdown: SharedShutdown,
) -> anyhow::Result<()> {
    let dump_manual_trigger_tx = setup_dump_manual_trigger_signal();

    let base_offset = config.upstream.reconnect_base_offset_secs.unwrap_or(0);
    let region_offset = config.upstream.reconnect_region_offset_secs.unwrap_or(0);

    // Per-region growth-timer subscription channels. The senders live in the
    // demux task; each receiver is drained by its region's connection task and
    // persists across that region's reconnects.
    let mut growth_sub_txs: HashMap<u8, UnboundedSender<Vec<u64>>> = HashMap::new();

    let mut handles = Vec::new();
    for (index, region) in config.upstream.regions.iter().enumerate() {
        let region = region.clone();
        let config = config.clone();
        let tx = tx.clone();
        let dump_tx = dump_tx.clone();
        let shutdown = shutdown.clone();
        let dump_manual_trigger_tx = dump_manual_trigger_tx.clone();
        let reconnect_offset = Duration::from_secs(base_offset + region_offset * index as u64);
        let (region_sub_tx, region_sub_rx) = unbounded_channel::<Vec<u64>>();
        growth_sub_txs.insert(region.id, region_sub_tx);
        handles.push(tokio::spawn(async move {
            connection::run_region(
                config,
                region,
                reconnect_offset,
                tx,
                dump_tx,
                region_sub_rx,
                shutdown,
                dump_manual_trigger_tx,
            )
            .await
        }));
    }

    // Demux processor requests to the owning region's channel. Runs until the
    // processor drops its sender (shutdown), then exits; dropping the per-region
    // senders simply disables the growth-sub branch in each connection task.
    tokio::spawn(demux_growth_subs(growth_sub_rx, growth_sub_txs));
    // Wait for all region tasks; first hard error propagates after the rest finish.
    let mut first_err: Option<anyhow::Error> = None;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_err.get_or_insert(e);
            }
            Err(e) => {
                first_err.get_or_insert(anyhow::anyhow!(e));
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Forward each [`GrowthTimerSubRequest`] to the connection task for its
/// region. Requests for unknown regions are dropped; a region whose task has
/// already exited (its receiver closed) simply drops the request.
async fn demux_growth_subs(
    mut rx: UnboundedReceiver<GrowthTimerSubRequest>,
    txs: HashMap<u8, UnboundedSender<Vec<u64>>>,
) {
    while let Some(req) = rx.recv().await {
        if req.entity_ids.is_empty() {
            continue;
        }
        if let Some(tx) = txs.get(&req.region_id) {
            let _ = tx.send(req.entity_ids);
        }
    }
}

fn setup_dump_manual_trigger_signal() -> Option<broadcast::Sender<()>> {
    #[cfg(unix)]
    {
        let (tx, _) = broadcast::channel(16);
        spawn_dump_manual_trigger_listener(tx.clone());
        return Some(tx);
    }

    #[cfg(not(unix))]
    {
        None
    }
}

#[cfg(unix)]
fn spawn_dump_manual_trigger_listener(tx: broadcast::Sender<()>) {
    tokio::spawn(async move {
        let mut stream = match signal(SignalKind::user_defined1()) {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "[upstream] failed to install SIGUSR1 listener for dump tasks: {}",
                    e
                );
                return;
            }
        };

        while stream.recv().await.is_some() {
            info!("[upstream] SIGUSR1 received; triggering dump tasks");
            let _ = tx.send(());
        }
    });
}
