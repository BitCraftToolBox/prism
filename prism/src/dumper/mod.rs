//! Dumper subsystem — serializes upstream table snapshots to JSON files on disk.
//!
//! Receives [`DumpMsg`] messages from region connection tasks whenever an
//! on-demand subscription delivers its initial rows.  Each message is written
//! to `{output_dir}/{module_name}/{table_name}.json`.

pub mod table_extract;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use log::{error, info};
use tokio::sync::mpsc::Receiver;

use crate::config::Config;
use crate::shutdown::SharedShutdown;

pub fn dumper_capacity(_config: &Config) -> usize {
    64
}

/// A batch of serialized rows ready to be written to disk.
pub struct DumpMsg {
    /// The upstream module (region) name — used as a subdirectory when no
    /// `output_folder` override is set.
    pub module_name: String,
    /// The table name — used as the JSON filename (without extension).
    pub table_name: String,
    /// Subdirectory override.  When `Some`, this folder name is used instead
    /// of `module_name` (still rooted under `dumper.output_dir`).
    pub output_folder: Option<String>,
    pub output_file: Option<String>,
    /// Serialised rows.
    pub rows: Vec<serde_json::Value>,
}

/// Run the dumper sink until shutdown is signaled or the channel closes.
pub async fn run(
    config: Arc<Config>,
    mut rx: Receiver<DumpMsg>,
    shutdown: SharedShutdown,
) -> Result<()> {
    let output_dir = PathBuf::from(&config.dumper.output_dir);

    let Some(shutdown_signal) = shutdown.lock().await.register() else {
        return Ok(());
    };
    tokio::pin!(shutdown_signal);

    info!("dumper: started, output_dir={}", output_dir.display());

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown_signal => {
                info!("dumper: shutdown signal received");
                break;
            }

            msg = rx.recv() => {
                let Some(msg) = msg else {
                    info!("dumper: channel closed");
                    break;
                };
                let folder = msg.output_folder.as_deref().unwrap_or(&msg.module_name);
                let dir = output_dir.join(folder);
                if let Err(e) = std::fs::create_dir_all(&dir) {
                    error!("dumper: failed to create directory {}: {e:?}", dir.display());
                    continue;
                }
                let file = msg.output_file.as_deref().unwrap_or(&msg.table_name);
                let path = dir.join(format!("{}.json", file));
                match serde_json::to_string(&msg.rows) {
                    Ok(json) => {
                        if let Err(e) = std::fs::write(&path, &json) {
                            error!("dumper: failed to write {}: {e:?}", path.display());
                        } else {
                            info!(
                                "dumper: wrote {} rows → {}",
                                msg.rows.len(),
                                path.display()
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "dumper: serialisation failed for {}/{}: {e:?}",
                            msg.module_name, msg.table_name
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
