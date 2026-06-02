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

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use upstream_bindings::region::DbUpdate;

use crate::config::Config;
use crate::shutdown::SharedShutdown;

/// A region update destined for the processor. Carries the originating region
/// id and the sync phase at the moment it was drained from the cacheless
/// channel.
pub struct RegionUpdate {
    pub region_id: u8,
    pub phase: Phase,
    pub update: DbUpdate,
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
    shutdown: SharedShutdown,
) -> anyhow::Result<()> {
    let mut handles = Vec::new();
    for region in &config.upstream.regions {
        let region = region.clone();
        let config = config.clone();
        let tx = tx.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            connection::run_region(config, region, tx, shutdown).await
        }));
    }
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
