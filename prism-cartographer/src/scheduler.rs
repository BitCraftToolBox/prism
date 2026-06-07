//! Cron scheduler for cartographer render tasks.
//!
//! Each task has an independent cron expression (6-field, seconds first)
//! and runs its renderer in a `spawn_blocking` call so the async runtime
//! is never blocked by CPU-intensive work.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use cron::Schedule;
use log::{error, info, warn};

use crate::config::{Config, RendererKind};

/// Start all scheduled render tasks and run until the process is interrupted.
pub async fn run(config: Arc<Config>) -> Result<()> {
    if config.tasks.is_empty() {
        warn!("[scheduler] no tasks configured; nothing to do");
        return Ok(());
    }

    let mut handles = Vec::new();
    for task in &config.tasks {
        let schedule_str = task.schedule.clone();
        let renderer = task.renderer;
        let cfg = config.clone();

        let handle = tokio::spawn(run_task(schedule_str, renderer, cfg));
        handles.push(handle);
    }

    // Run until all tasks finish (they run indefinitely unless interrupted).
    for handle in handles {
        if let Err(e) = handle.await {
            error!("[scheduler] task panicked: {:?}", e);
        }
    }

    Ok(())
}

async fn run_task(schedule_str: String, renderer: RendererKind, config: Arc<Config>) {
    let schedule = match Schedule::from_str(&schedule_str) {
        Ok(s) => s,
        Err(e) => {
            error!(
                "[scheduler] invalid cron expression {:?} for {}: {}",
                schedule_str, renderer, e
            );
            return;
        }
    };

    info!("[scheduler] {} scheduled with {:?}", renderer, schedule_str);

    loop {
        let delay = match schedule.upcoming(Utc).next() {
            Some(next) => (next - Utc::now()).to_std().unwrap_or(Duration::ZERO),
            None => {
                error!(
                    "[scheduler] cron {:?} for {} has no future occurrences",
                    schedule_str, renderer
                );
                return;
            }
        };

        info!(
            "[scheduler] {} sleeping {:?} until next run",
            renderer, delay
        );
        tokio::time::sleep(delay).await;

        info!("[scheduler] {} starting", renderer);
        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || run_renderer(renderer, &cfg)).await;

        match result {
            Ok(Ok(())) => info!("[scheduler] {} completed successfully", renderer),
            Ok(Err(e)) => error!("[scheduler] {} failed: {:#}", renderer, e),
            Err(e) => error!("[scheduler] {} task panicked: {:?}", renderer, e),
        }
    }
}

fn run_renderer(renderer: RendererKind, config: &Config) -> Result<()> {
    let input_dir = &config.input_dir;
    let region_prefix = &config.region_prefix;
    let output_dir = &config.output_dir;

    match renderer {
        RendererKind::Terrain => {
            crate::renderers::terrain::render(input_dir, region_prefix, output_dir)
        }
        RendererKind::Game => crate::renderers::game::render(output_dir),
        RendererKind::Roads => {
            crate::renderers::roads::render(input_dir, region_prefix, output_dir)
        }
    }
}
