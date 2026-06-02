//! Per-region connection lifecycle: connect → subscribe → forward updates →
//! reconnect on error → exit cleanly on shutdown.

use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::time::Duration;

use anyhow::Result;
use log::{error, info, warn};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use upstream_bindings::ext::ctx::RunUntil;
use upstream_bindings::region::{DbConnection, DbUpdate};

use super::subscription::{enabled_pipelines, queue_subscribe};
use super::{Phase, RegionUpdate, SharedPhase, load_phase, store_phase};
use crate::config::{Config, RegionConfig};
use crate::shutdown::SharedShutdown;

const UPSTREAM_URI: &str = "https://bitcraft-early-access.spacetimedb.com";
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

pub async fn run_region(
    config: Arc<Config>,
    region: RegionConfig,
    proc_tx: UnboundedSender<RegionUpdate>,
    shutdown: SharedShutdown,
) -> Result<()> {
    let token = match config.token_for(&region) {
        Some(t) => t.to_string(),
        None => {
            warn!("[{}] no token available; skipping region", region.name);
            return Ok(());
        }
    };
    let host = if config.upstream.host.is_empty() {
        UPSTREAM_URI.to_string()
    } else {
        config.upstream.host.clone()
    };
    let pipelines = enabled_pipelines(&config.pipelines);
    if pipelines.is_empty() {
        warn!("[{}] no pipelines enabled; skipping region", region.name);
        return Ok(());
    }

    loop {
        // Per-connection phase shared between the connection task and the
        // channel-drain task.
        let phase: SharedPhase = Arc::new(AtomicU8::new(Phase::Syncing as u8));

        // The cacheless update channel is private to one connection — we
        // drain it from a helper task that re-emits tagged updates.
        let (cache_tx, mut cache_rx) = unbounded_channel::<DbUpdate>();
        let drain_phase = phase.clone();
        let drain_tx = proc_tx.clone();
        let drain_region = region.id;
        let drain = tokio::spawn(async move {
            while let Some(update) = cache_rx.recv().await {
                let _ = drain_tx.send(RegionUpdate {
                    region_id: drain_region,
                    phase: load_phase(&drain_phase),
                    update,
                });
            }
        });

        let pipelines_for_connect = pipelines.clone();
        let phase_for_connect = phase.clone();
        let region_name_for_log = region.name.clone();
        let region_name_for_disconnect = region.name.clone();

        info!("[{}] connecting...", region.name);
        let built = DbConnection::builder()
            .with_uri(&host)
            .with_module_name(&region.name)
            .with_token(Some(&token))
            .with_light_mode(true)
            .with_channel(cache_tx.clone())
            .on_connect(move |ctx, _id, _tok| {
                info!(
                    "[{}] connected; starting subscriptions",
                    region_name_for_log
                );
                let phase = phase_for_connect.clone();
                let region_name = region_name_for_log.clone();
                queue_subscribe(
                    ctx,
                    &region_name_for_log,
                    pipelines_for_connect.clone(),
                    move || {
                        info!("[{}] all pipelines live", region_name);
                        store_phase(&phase, Phase::Live);
                    },
                );
            })
            .on_disconnect(move |_ectx, err| match err {
                Some(e) => warn!("[{}] disconnected: {:?}", region_name_for_disconnect, e),
                None => info!("[{}] disconnected", region_name_for_disconnect),
            })
            .build();

        match built {
            Ok(con) => {
                let Some(signal) = shutdown.lock().await.register() else {
                    drop(cache_tx);
                    let _ = drain.await;
                    return Ok(());
                };
                if let Err(e) = con.run_until(signal).await {
                    error!("[{}] connection ended with error: {:?}", region.name, e);
                }
            }
            Err(e) => {
                error!("[{}] failed to build connection: {:?}", region.name, e);
            }
        }

        // Wake the drain task by dropping the original sender side; it will
        // exit once the cacheless channel is closed inside the SDK on disconnect.
        drop(cache_tx);
        let _ = drain.await;

        // Check shutdown — if triggered while we were running, exit.
        {
            let mut sd = shutdown.lock().await;
            if sd.register().is_none() {
                return Ok(());
            }
            // register() returned a fresh receiver because we're still alive;
            // we don't need it here, drop it.
        }

        warn!("[{}] reconnecting in {:?}", region.name, RECONNECT_BACKOFF);
        // Wait the backoff but bail early on shutdown.
        let Some(signal) = shutdown.lock().await.register() else {
            return Ok(());
        };
        tokio::select! {
            _ = signal => return Ok(()),
            _ = tokio::time::sleep(RECONNECT_BACKOFF) => {}
        }
    }
}
