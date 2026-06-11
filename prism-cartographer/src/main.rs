mod config;
mod renderers;
mod scheduler;
mod shutdown;
mod tile_generator;

use std::sync::Arc;

use anyhow::{Context, Result};

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    log::info!("[cartographer] loading config from {}", config_path);

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read config file: {}", config_path))?;

    let config: config::Config = toml::from_str(&raw)
        .with_context(|| format!("Failed to parse config file: {}", config_path))?;

    log::info!("[cartographer] input_dir: {}", config.input_dir.display());
    log::info!("[cartographer] output_dir: {}", config.output_dir.display());
    log::info!("[cartographer] region_prefix: {}", config.region_prefix);
    log::info!(
        "[cartographer] run_on_complete: {}",
        config.run_on_complete.as_deref().unwrap_or("None")
    );
    log::info!("[cartographer] tasks: {}", config.tasks.len());
    for task in &config.tasks {
        log::info!("  {} @ {}", task.renderer, task.schedule);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to build Tokio runtime")?;

    rt.block_on(async {
        let sd = shutdown::Shutdown::new();
        shutdown::install_ctrl_c(sd.clone());
        scheduler::run(Arc::new(config), sd).await
    })?;

    Ok(())
}
