use serde::Deserialize;
use std::path::PathBuf;

/// Top-level cartographer configuration.
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Directory containing per-region dump subdirectories.
    pub input_dir: PathBuf,

    /// Prefix used for region subdirectory names.
    /// Each region is loaded from `{input_dir}/{region_prefix}{N}/`.
    #[serde(default = "default_region_prefix")]
    pub region_prefix: String,

    /// Root directory for all rendered output.
    pub output_dir: PathBuf,

    /// Scheduled rendering tasks.
    #[serde(default)]
    pub tasks: Vec<TaskConfig>,
}

fn default_region_prefix() -> String {
    "bitcraft-live-".to_string()
}

/// A single scheduled render task.
#[derive(Debug, Deserialize, Clone)]
pub struct TaskConfig {
    /// Which renderer to run.
    pub renderer: RendererKind,

    /// Cron expression (6-field: sec min hour dom month dow).
    pub schedule: String,
}

/// The available renderer types.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RendererKind {
    Terrain,
    Game,
    Roads,
}

impl std::fmt::Display for RendererKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RendererKind::Terrain => write!(f, "terrain"),
            RendererKind::Game => write!(f, "game"),
            RendererKind::Roads => write!(f, "roads"),
        }
    }
}
