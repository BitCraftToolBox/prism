//! Cron scheduler for cartographer render tasks.
//!
//! Each task has an independent cron expression (6-field, seconds first) and
//! runs its renderer in a `spawn_blocking` call.
//!
//! Output is written to a temporary directory alongside the real tiles
//! directory.  Only when the render completes *and* no shutdown has been
//! requested is the temp directory atomically swapped into place.  This
//! ensures the live tiles are never left in a partially-rendered state.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use cron::Schedule;
use log::{error, info, warn};

use crate::config::{Config, RendererKind};
use crate::shutdown::SharedShutdown;

/// Start all scheduled render tasks and drive them until shutdown.
pub async fn run(config: Arc<Config>, shutdown: SharedShutdown) -> Result<()> {
    if config.tasks.is_empty() {
        warn!("[scheduler] no tasks configured; nothing to do");
        return Ok(());
    }

    let mut handles = Vec::new();
    for task in &config.tasks {
        let schedule_str = task.schedule.clone();
        let renderer = task.renderer;
        let cfg = config.clone();
        let sd = shutdown.clone();

        let handle = tokio::spawn(run_task(schedule_str, renderer, cfg, sd));
        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("[scheduler] task panicked: {:?}", e);
        }
    }

    Ok(())
}

async fn run_task(
    schedule_str: String,
    renderer: RendererKind,
    config: Arc<Config>,
    shutdown: SharedShutdown,
) {
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

    loop {
        // Register a shutdown receiver for the sleep phase.
        let Some(shutdown_rx) = shutdown.lock().await.register() else {
            return; // already shutting down
        };

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

        // Wait for the trigger time; bail immediately on shutdown.
        let should_run = tokio::select! {
            biased;
            _ = shutdown_rx => false,
            _ = tokio::time::sleep(delay) => true,
        };

        if !should_run {
            return;
        }

        let real_tiles_dir = tiles_dir_for(renderer, &config.output_dir);
        let tiles_parent = match real_tiles_dir.parent() {
            Some(p) => p.to_path_buf(),
            None => config.output_dir.clone(),
        };

        if let Err(e) = std::fs::create_dir_all(&tiles_parent)
            .with_context(|| format!("Failed to create {}", tiles_parent.display()))
        {
            error!("[scheduler] {}: {:#}", renderer, e);
            continue;
        }

        // Name: ".<renderer>-<nanos>.tmp" — unique per run, same filesystem as
        // the real output so rename() is always an in-place operation.
        let temp_name = format!(
            ".{}-{}.tmp",
            renderer,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let temp_tiles_dir = tiles_parent.join(&temp_name);

        if let Err(e) = std::fs::create_dir_all(&temp_tiles_dir)
            .with_context(|| format!("Failed to create temp dir {}", temp_tiles_dir.display()))
        {
            error!("[scheduler] {}: {:#}", renderer, e);
            continue;
        }

        // ── run the renderer in a blocking thread ─────────────────────────────
        // Grab the cancel flag before spawn_blocking so the renderer can poll
        // it without any async machinery.
        let cancel = shutdown.lock().await.cancel_flag();

        info!(
            "[scheduler] {} starting → {}",
            renderer,
            temp_tiles_dir.display()
        );
        let cfg = config.clone();
        let tmp = temp_tiles_dir.clone();
        let result =
            tokio::task::spawn_blocking(move || run_renderer(renderer, &cfg, &tmp, &cancel)).await;

        // ── check shutdown after the blocking work completes ──────────────────
        // The renderer bails early when it sees the cancel flag; we check the
        // shutdown state here to decide whether to commit or discard.
        let is_shutdown = shutdown.lock().await.is_triggered();

        match result {
            Ok(Ok(())) if !is_shutdown => {
                // Clean finish, no shutdown — commit atomically.
                match commit(&temp_tiles_dir, &real_tiles_dir) {
                    Ok(()) => {
                        info!("[scheduler] {} done, output committed", renderer);
                        run_on_complete(&config, &real_tiles_dir, renderer).await;
                    }
                    Err(e) => {
                        error!("[scheduler] {} commit failed: {:#}", renderer, e);
                        let _ = std::fs::remove_dir_all(&temp_tiles_dir);
                    }
                }
            }
            Ok(Ok(())) => {
                // Render finished after shutdown was requested — discard.
                info!(
                    "[scheduler] {} render complete but shutdown requested; discarding output",
                    renderer
                );
                let _ = std::fs::remove_dir_all(&temp_tiles_dir);
                return;
            }
            Ok(Err(e)) if is_shutdown => {
                // Renderer bailed early due to cancel flag — expected.
                info!("[scheduler] {} canceled: {}", renderer, e);
                let _ = std::fs::remove_dir_all(&temp_tiles_dir);
                return;
            }
            Ok(Err(e)) => {
                error!("[scheduler] {} failed: {:#}", renderer, e);
                let _ = std::fs::remove_dir_all(&temp_tiles_dir);
            }
            Err(e) => {
                error!("[scheduler] {} task panicked: {:?}", renderer, e);
                let _ = std::fs::remove_dir_all(&temp_tiles_dir);
            }
        }

        if is_shutdown {
            return;
        }
    }
}

/// Replace `real_tiles_dir` with `temp_tiles_dir` via remove-then-rename.
fn commit(temp_tiles_dir: &Path, real_tiles_dir: &Path) -> Result<()> {
    if real_tiles_dir.exists() {
        std::fs::remove_dir_all(real_tiles_dir)
            .with_context(|| format!("Failed to remove {}", real_tiles_dir.display()))?;
    }
    std::fs::rename(temp_tiles_dir, real_tiles_dir).with_context(|| {
        format!(
            "Failed to rename {} → {}",
            temp_tiles_dir.display(),
            real_tiles_dir.display()
        )
    })?;
    Ok(())
}

/// The final tiles directory for a given renderer under `output_dir`.
fn tiles_dir_for(renderer: RendererKind, output_dir: &Path) -> PathBuf {
    match renderer {
        RendererKind::Terrain => output_dir.join("maps").join("terrain").join("tiles"),
        RendererKind::Game => output_dir.join("maps").join("game").join("tiles"),
        RendererKind::Roads => output_dir.join("roads").join("tiles"),
    }
}

/// If `config.run_on_complete` is set, format it with the committed path and
/// run it via `sh -c`.  Two placeholders are expanded before the command is
/// executed:
///
/// * `{}` — absolute path of the committed output directory
///           (e.g. `/data/roads/tiles`)
/// * `{root}` — absolute path of `config.output_dir`, the common root that
///              all renderer outputs live under (e.g. `/data`).  Together with
///              `{}` the script can derive the relative sub-path
///              (`roads/tiles`) and recreate the same directory hierarchy on
///              the remote side.
///
/// Example config:
///   run_on_complete = "/app/upload_tiles.sh {} {root}"
async fn run_on_complete(config: &Config, committed_path: &Path, renderer: RendererKind) {
    let Some(template) = config.run_on_complete.as_deref() else {
        return;
    };

    let path_str = committed_path.to_string_lossy();
    let root_str = config.output_dir.to_string_lossy();
    let cmd = template
        .replace("{root}", &root_str)
        .replace("{}", &path_str);

    info!(
        "[scheduler] {} running post-commit command: {}",
        renderer, cmd
    );

    match tokio::process::Command::new("sh")
        .args(["-c", &cmd])
        .status()
        .await
    {
        Ok(status) if status.success() => {
            info!("[scheduler] {} post-commit command succeeded", renderer);
        }
        Ok(status) => {
            error!(
                "[scheduler] {} post-commit command exited with status {}",
                renderer, status
            );
        }
        Err(e) => {
            error!(
                "[scheduler] {} post-commit command failed to start: {}",
                renderer, e
            );
        }
    }
}

/// Synchronous entry point called inside `spawn_blocking`.
fn run_renderer(
    renderer: RendererKind,
    config: &Config,
    tiles_dir: &Path,
    canceled: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    match renderer {
        RendererKind::Terrain => crate::renderers::terrain::render(
            &config.input_dir,
            &config.region_prefix,
            tiles_dir,
            canceled,
        ),
        RendererKind::Game => crate::renderers::game::render(tiles_dir, canceled),
        RendererKind::Roads => crate::renderers::roads::render(
            &config.input_dir,
            &config.region_prefix,
            tiles_dir,
            canceled,
        ),
    }
}
