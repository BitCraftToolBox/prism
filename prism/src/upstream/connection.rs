//! Per-region connection lifecycle: connect → subscribe → forward updates →
//! reconnect on error → exit cleanly on shutdown.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use cron::Schedule;
use log::{debug, error, info, warn};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Sender, UnboundedSender, unbounded_channel};
use upstream_bindings::ext::ctx::RunUntil;
use upstream_bindings::region::{DbConnection, DbUpdate};
use upstream_bindings::sdk::DbContext;

use super::subscription::{enabled_pipelines, queue_subscribe};
use super::{Phase, RegionUpdate, SharedPhase, load_phase, store_phase};
use crate::config::{Config, DumpScheduleConfig, RegionConfig};
use crate::dumper::table_extract::SupportedTable;
use crate::dumper::{DumpMsg, table_extract};
use crate::shutdown::SharedShutdown;

const UPSTREAM_URI: &str = "https://bitcraft-early-access.spacetimedb.com";
const RECONNECT_BACKOFF_STEPS: [Duration; 5] = [
    Duration::from_secs(5),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(180),
    Duration::from_secs(300),
];
/// Maximum time to wait for a dump subscription to deliver rows.
const DUMP_TIMEOUT: Duration = Duration::from_secs(60);

fn error_is_normal_disconnect(e: &upstream_bindings::sdk::Error) -> bool {
    matches!(e, upstream_bindings::sdk::Error::Disconnected)
}

pub async fn run_region(
    config: Arc<Config>,
    region: RegionConfig,
    proc_tx: UnboundedSender<RegionUpdate>,
    dump_tx: Sender<DumpMsg>,
    shutdown: SharedShutdown,
    dump_manual_trigger_tx: Option<broadcast::Sender<()>>,
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
    // Spawn one persistent dump-schedule task per schedule entry.  Each task
    // creates its own short-lived connection when the interval fires so that
    // dump subscriptions are completely independent of the main connection.
    for cfg in config.dumps_for(&region) {
        let manual_trigger_rx = dump_manual_trigger_tx.as_ref().map(|tx| tx.subscribe());
        tokio::spawn(run_dump_schedule(
            host.clone(),
            region.name.clone(),
            token.clone(),
            cfg.clone(),
            dump_tx.clone(),
            shutdown.clone(),
            manual_trigger_rx,
        ));
    }

    let pipelines = enabled_pipelines(&config.pipelines);
    if pipelines.is_empty() {
        warn!("[{}] no pipelines enabled; skipping region", region.name);
        return Ok(());
    }

    let mut reconnect_backoff_idx = 0usize;

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
        let connected_this_attempt = Arc::new(AtomicBool::new(false));
        let connected_flag = connected_this_attempt.clone();

        info!("[{}] connecting...", region.name);
        let built = DbConnection::builder()
            .with_uri(&host)
            .with_module_name(&region.name)
            .with_token(Some(&token))
            .with_light_mode(true)
            .with_channel(cache_tx.clone())
            .on_connect(move |ctx, _id, _tok| {
                connected_flag.store(true, Ordering::Relaxed);
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
                Some(e) if error_is_normal_disconnect(&e) => {
                    info!("[{}] disconnected", region_name_for_disconnect)
                }
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

        let connected = connected_this_attempt.load(Ordering::Relaxed);
        if connected {
            reconnect_backoff_idx = 0;
        }
        let reconnect_backoff = RECONNECT_BACKOFF_STEPS[reconnect_backoff_idx];
        if !connected {
            reconnect_backoff_idx =
                (reconnect_backoff_idx + 1).min(RECONNECT_BACKOFF_STEPS.len() - 1);
        }

        warn!("[{}] reconnecting in {:?}", region.name, reconnect_backoff);
        // Wait the backoff but bail early on shutdown.
        let Some(signal) = shutdown.lock().await.register() else {
            return Ok(());
        };
        tokio::select! {
            _ = signal => return Ok(()),
            _ = tokio::time::sleep(reconnect_backoff) => {}
        }
    }
}

/// Runs indefinitely on a timer.  Each interval it opens a fresh, short-lived
/// upstream connection, subscribes to the configured tables, waits for the
/// initial rows to arrive, serializes them and forwards to the dumper.
async fn run_dump_schedule(
    host: String,
    module_name: String,
    token: String,
    cfg: DumpScheduleConfig,
    dump_tx: Sender<DumpMsg>,
    shutdown: SharedShutdown,
    mut manual_trigger_rx: Option<broadcast::Receiver<()>>,
) {
    let schedule = match Schedule::from_str(&cfg.schedule) {
        Ok(s) => s,
        Err(e) => {
            error!(
                "[{}] dump: invalid cron expression {:?}: {}",
                module_name, cfg.schedule, e
            );
            return;
        }
    };

    // Validate configured tables against the supported set upfront, so we fail
    // fast rather than silently producing no output every run.
    let tables: Vec<(SupportedTable, &crate::config::DumpTableConfig)> = cfg
        .tables
        .iter()
        .filter_map(|t| match SupportedTable::from_name(&t.name) {
            Some(supported) => Some((supported, t)),
            None => {
                warn!(
                    "[{}] dump: table {:?} is not supported and will be skipped. \
                     Supported tables: {:?}",
                    module_name,
                    t.name,
                    SupportedTable::ALL
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                );
                None
            }
        })
        .collect();

    if tables.is_empty() {
        warn!(
            "[{}] dump: no supported tables configured for schedule {:?}; task exiting",
            module_name, cfg.schedule
        );
        return;
    }

    let Some(shutdown_signal) = shutdown.lock().await.register() else {
        return;
    };
    tokio::pin!(shutdown_signal);

    loop {
        // Compute sleep duration until the next scheduled fire time.
        let delay = match schedule.upcoming(Utc).next() {
            Some(next) => (next - Utc::now()).to_std().unwrap_or(Duration::ZERO),
            None => {
                error!(
                    "[{}] dump: cron {:?} has no future occurrences",
                    module_name, cfg.schedule
                );
                return;
            }
        };
        let table_names: Vec<&str> = tables.iter().map(|(t, _)| t.as_str()).collect();

        debug!(
            "[{}] dumper for {:?} sleeping {:?}",
            module_name, table_names, delay
        );

        let triggered_by_manual_signal = match manual_trigger_rx.as_mut() {
            Some(rx) => tokio::select! {
                biased;
                _ = &mut shutdown_signal => return,
                _ = tokio::time::sleep(delay) => false,
                _ = wait_for_manual_trigger(rx) => true,
            },
            None => {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_signal => return,
                    _ = tokio::time::sleep(delay) => false,
                }
            }
        };

        if triggered_by_manual_signal {
            debug!(
                "[{}] dump: SIGUSR1 manual trigger received; running now for tables {:?}",
                module_name, table_names
            );
        }

        info!(
            "[{}] dump: starting scheduled dump for tables {:?}",
            module_name, table_names
        );

        // Open a short-lived connection purely for this dump.
        let (cache_tx, mut cache_rx) = unbounded_channel::<DbUpdate>();
        let tables_for_connect = cfg.tables.clone();
        let module_for_log = module_name.clone();

        let built = DbConnection::builder()
            .with_uri(&host)
            .with_module_name(&module_name)
            .with_token(Some(&token))
            .with_light_mode(true)
            .with_channel(cache_tx.clone())
            .on_connect(move |ctx, _id, _tok| {
                info!(
                    "[{}] dump: connected, subscribing to tables",
                    module_for_log
                );
                let queries: Vec<String> = tables_for_connect
                    .iter()
                    .map(|t| match &t.query {
                        Some(q) => q.to_string(),
                        None => format!("SELECT * FROM {};", t.name),
                    })
                    .collect();
                ctx.subscription_builder().subscribe(queries);
            })
            .on_disconnect(|_, _| {})
            .build();

        let con = match built {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "[{}] dump: failed to build connection: {:?}",
                    module_name, e
                );
                continue;
            }
        };

        // Drive the dump connection in a background task; kill it via signal.
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
        let con_task = tokio::spawn(async move {
            let _ = con.run_until(signal_rx).await;
        });

        // Build a SupportedTable → config map for fast lookup during receive.
        let mut pending: hashbrown::HashMap<SupportedTable, &crate::config::DumpTableConfig> =
            tables.iter().map(|(t, cfg)| (*t, *cfg)).collect();
        let deadline = tokio::time::sleep(DUMP_TIMEOUT);
        tokio::pin!(deadline);

        // We only expect a single InitialSubscription message: one subscription covers
        // all queries, so all rows for all tables arrive in one update.  Wait for that
        // message, a timeout, or a shutdown signal — then fall through to teardown.
        tokio::select! {
            biased;
            _ = &mut shutdown_signal => {
                info!("[{}] dump: shutdown signal received, terminating dump", module_name);
            }
            _ = &mut deadline => {
                if !pending.is_empty() {
                    let names: Vec<&str> = pending.keys().map(|t| t.as_str()).collect();
                    warn!(
                        "[{}] dump: timed out waiting for tables {:?} (no rows?)",
                        module_name, names
                    );
                }
            }
            upd = cache_rx.recv() => {
                if let Some(upd) = upd {
                    let arrived: Vec<SupportedTable> = pending
                        .keys()
                        .copied()
                        .filter(|t| table_extract::has_inserts(&upd, *t))
                        .collect();
                    for table in arrived {
                        let table_cfg = pending.remove(&table).unwrap();
                        let rows = table_extract::extract_rows_json(&upd, table);
                        info!(
                            "[{}] dump: {} rows received for table '{}'",
                            module_name,
                            rows.len(),
                            table.as_str()
                        );
                        let msg = DumpMsg {
                            module_name: module_name.clone(),
                            table_name: table.as_str().to_string(),
                            output_folder: table_cfg.output_folder.clone(),
                            output_file: table_cfg.output_file.clone(),
                            rows,
                        };
                        if let Err(e) = dump_tx.try_send(msg) {
                            warn!("[{}] dump: channel send failed: {:?}", module_name, e);
                        }
                    }
                    if !pending.is_empty() {
                        let names: Vec<&str> = pending.keys().map(|t| t.as_str()).collect();
                        info!("[{}] dump: subscription received with no rows for tables: {:?}", module_name, names);
                    }
                }
            }
        }

        // Tear down the dump connection.
        let _ = signal_tx.send(());
        let _ = con_task.await;

        // Exit early if shutdown was triggered.
        if shutdown.lock().await.register().is_none() {
            return;
        }
    }
}

async fn wait_for_manual_trigger(rx: &mut broadcast::Receiver<()>) {
    loop {
        match rx.recv().await {
            Ok(()) => return,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    "[dump] SIGUSR1 trigger lagged by {} signal(s); running immediately",
                    skipped
                );
                return;
            }
            // Sender lifetime is tied to upstream::run_all(); if closed, disable trigger wakeups.
            Err(broadcast::error::RecvError::Closed) => {
                std::future::pending::<()>().await;
            }
        }
    }
}
