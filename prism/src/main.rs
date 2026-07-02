//! Prism — BitCraft multi-region relay & historical pipeline.

mod config;
mod dumper;
mod history;
mod processor;
mod relay;
mod shutdown;
mod upstream;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use log::{error, info};
use mimalloc::MiMalloc;
use tokio::sync::mpsc::unbounded_channel;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Histogram buckets for the prism latency metrics (relay/history flush,
/// processor latency). These render the `metrics` histograms as native
/// Prometheus histograms (`_bucket`/`le`) instead of the default rolling
/// summaries, so the dashboards' `histogram_quantile(...)` queries work and
/// the data does not decay to zero between scrapes. Range spans sub-microsecond
/// (relay flushes) to ~1s (worst-case flushes).
const LATENCY_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.0025,
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config_path = std::env::var("PRISM_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = Arc::new(config::Config::load(PathBuf::from(&config_path))?);
    info!(
        "config loaded: regions={} relay_module={}",
        config.upstream.regions.len(),
        config.relay.as_ref().map_or("none", |r| r.module.as_str()),
    );

    if let Some(m) = &config.metrics {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .add_global_label("node", m.node.clone())
            .set_buckets(LATENCY_BUCKETS)
            .expect("valid histogram buckets")
            .with_http_listener(([0, 0, 0, 0], m.port))
            .install()
            .expect("metrics recorder");
        info!("metrics: listening on :{}", m.port);
    }

    let shutdown = shutdown::Shutdown::new();
    shutdown::install_ctrl_c(shutdown.clone());

    // Upstream → processor channel (unbounded — backpressure happens at the
    // per-sink processor→sink channels instead).
    let (upstream_tx, upstream_rx) = unbounded_channel();
    // Processor → sink channels (bounded).
    let (proc_handle, sinks) = processor::channels(&config);
    // upstream → dumper channel (bounded).
    let (dump_tx, dump_rx) = tokio::sync::mpsc::channel(dumper::dumper_capacity(&config));

    // Spawn upstream, processor, relay, history, dumper concurrently. Each
    // gets a clone of the shared shutdown coordinator; first hard error
    // triggers shutdown for everyone.
    let up = tokio::spawn(spawn_subsystem(
        "upstream",
        upstream::run_all(config.clone(), upstream_tx, dump_tx, shutdown.clone()),
        shutdown.clone(),
    ));
    let proc = tokio::spawn(spawn_subsystem(
        "processor",
        processor::run(config.clone(), upstream_rx, proc_handle, shutdown.clone()),
        shutdown.clone(),
    ));
    let relay = tokio::spawn(spawn_subsystem(
        "relay",
        relay::run(config.clone(), sinks.relay_rx, shutdown.clone()),
        shutdown.clone(),
    ));
    let history = tokio::spawn(spawn_subsystem(
        "history",
        history::run(config.clone(), sinks.history_rx, shutdown.clone()),
        shutdown.clone(),
    ));
    let dump = tokio::spawn(spawn_subsystem(
        "dumper",
        dumper::run(config.clone(), dump_rx, shutdown.clone()),
        shutdown.clone(),
    ));

    let _ = tokio::join!(up, proc, relay, history, dump);
    info!("all subsystems exited; bye");
    Ok(())
}

async fn spawn_subsystem<F>(
    name: &'static str,
    fut: F,
    shutdown: shutdown::SharedShutdown,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send,
{
    match fut.await {
        Ok(()) => {
            info!("subsystem={name} exited cleanly");
            Ok(())
        }
        Err(e) => {
            error!("subsystem={name} fatal error: {e:?}");
            shutdown.lock().await.trigger();
            Err(e)
        }
    }
}
