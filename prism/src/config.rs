//! Top-level config loaded from `config.toml` with env-var overrides.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub upstream: UpstreamConfig,
    /// Downstream relay module connection. Required when any pipeline is
    /// enabled; may be omitted when running in history- or dumper- only mode.
    #[serde(default)]
    pub relay: Option<RelayConfig>,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub pipelines: PipelinesConfig,
    #[serde(default)]
    pub dumper: DumperConfig,
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    #[serde(default = "default_upstream_host")]
    pub host: String,
    /// Default token used for any region without an explicit token. Optional;
    /// if unset, only regions with their own token will be connected to.
    #[serde(default)]
    pub default_token: Option<String>,
    /// Default dump schedules applied to every region that does not set its
    /// own `dumps` key.  A region with an explicit `dumps = []` (empty array)
    /// suppresses the default and runs no dumps; a region that omits the key
    /// entirely inherits this list.
    #[serde(default)]
    pub default_dump_schedule: Option<Vec<DumpScheduleConfig>>,
    pub regions: Vec<RegionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegionConfig {
    /// Module name, e.g. `bitcraft-live-1`.
    pub name: String,
    /// Numeric region id used to tag updates and key relay rows. By
    /// convention this matches the suffix of `name` but is set explicitly
    /// so it cannot drift.
    pub id: u8,
    /// Per-region token override. If unset, [`UpstreamConfig::default_token`]
    /// is used.
    #[serde(default)]
    pub token: Option<String>,
    /// Scheduled table dumps for this region.
    ///
    /// - **Absent** (`dumps` key omitted): inherits
    ///   [`UpstreamConfig::default_dump_schedule`] if set.
    /// - **Explicitly empty** (`dumps = []`): no dumps, even if a default is
    ///   configured.
    /// - **Non-empty**: uses these schedules exclusively.
    pub dumps: Option<Vec<DumpScheduleConfig>>,
}

/// One scheduled dump entry: on the given cron schedule, connect to the
/// upstream module and pull a fresh snapshot of each listed table.
/// The schedule uses 6-field cron syntax: `sec min hour dom month dow`
/// e.g. `"0 0 * * * *"` = every hour on the hour.
#[derive(Debug, Clone, Deserialize)]
pub struct DumpScheduleConfig {
    /// Cron expression (6-field: sec min hour dom month dow).
    pub schedule: String,
    /// Tables to snapshot on this schedule.
    pub tables: Vec<DumpTableConfig>,
}

/// Per-table dump configuration within a [`DumpScheduleConfig`].
#[derive(Debug, Clone, Deserialize)]
pub struct DumpTableConfig {
    /// Table name, e.g. `"terrain_chunk_state"`.
    pub name: String,
    /// Optional query override instead of `SELECT * FROM name;`.
    /// Allows custom joins, filters, etc.
    #[serde(default)]
    pub query: Option<String>,
    /// Optional output subdirectory override.  When set this, folder name is
    /// used instead of the module name, still rooted under `dumper.output_dir`.
    #[serde(default)]
    pub output_folder: Option<String>,
    /// Optional output file name override, instead of table name.
    #[serde(default)]
    pub output_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    pub uri: String,
    pub module: String,
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PipelinesConfig {
    #[serde(default = "default_true")]
    pub resources: bool,
    #[serde(default = "default_true")]
    pub growth_timers: bool,
    #[serde(default = "default_true")]
    pub enemies: bool,
    #[serde(default = "default_true")]
    pub players: bool,
    #[serde(default = "default_true")]
    pub crafts: bool,
    #[serde(default = "default_true")]
    pub claims: bool,
}

impl PipelinesConfig {
    /// Returns `true` if at least one pipeline is enabled and will emit
    /// messages to the relay sink.
    pub fn any_enabled(&self) -> bool {
        self.resources
            || self.growth_timers
            || self.enemies
            || self.players
            || self.crafts
            || self.claims
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DumperConfig {
    /// Directory to write snapshot files into. Defaults to `"/data"`.
    #[serde(default = "default_dump_dir")]
    pub output_dir: String,
}

impl Default for DumperConfig {
    fn default() -> Self {
        Self {
            output_dir: default_dump_dir(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Port for the Prometheus /metrics HTTP endpoint.
    pub port: u16,
    /// Value of the `node` label attached to every metric.
    pub node: String,
}

fn default_true() -> bool {
    true
}
fn default_upstream_host() -> String {
    "https://bitcraft-early-access.spacetimedb.com".into()
}
fn default_dump_dir() -> String {
    "/data/".into()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading config from {}", path.as_ref().display()))?;
        let mut cfg: Config = toml::from_str(&raw).context("parsing config.toml")?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(t) = std::env::var("PRISM_UPSTREAM_TOKEN") {
            self.upstream.default_token = Some(t);
        }
        if let Ok(t) = std::env::var("PRISM_RELAY_TOKEN")
            && let Some(ref mut relay) = self.relay
        {
            relay.token = Some(t);
        }
        if let Ok(u) = std::env::var("DATABASE_URL") {
            self.database.url = Some(u);
        }
    }

    fn validate(&self) -> Result<()> {
        if self.upstream.regions.is_empty() {
            return Err(anyhow!("config.upstream.regions is empty"));
        }
        for r in &self.upstream.regions {
            if r.token.is_none() && self.upstream.default_token.is_none() {
                return Err(anyhow!(
                    "region {:?} has no token and no global default_token is set",
                    r.name
                ));
            }
        }
        if self.pipelines.any_enabled() {
            match &self.relay {
                None => {
                    return Err(anyhow!(
                        "one or more pipelines are enabled but [relay] is not configured; \
                         add a [relay] section or disable all pipelines"
                    ));
                }
                Some(relay) if relay.token.is_none() => {
                    return Err(anyhow!(
                        "relay has no token; use config.toml or PRISM_RELAY_TOKEN env var"
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Token to use for a given region (per-region override → global default).
    pub fn token_for<'a>(&'a self, region: &'a RegionConfig) -> Option<&'a str> {
        region
            .token
            .as_deref()
            .or(self.upstream.default_token.as_deref())
    }

    /// Dump schedules to use for a given region.
    ///
    /// Resolution order:
    /// 1. Region has explicit `dumps` (even `[]`) → use it as-is.
    /// 2. Region omits `dumps` and a `default_dump_schedule` is set → use
    ///    the default.
    /// 3. Otherwise → empty slice (no dumps).
    pub fn dumps_for<'a>(&'a self, region: &'a RegionConfig) -> &'a [DumpScheduleConfig] {
        match &region.dumps {
            Some(v) => v.as_slice(),
            None => self
                .upstream
                .default_dump_schedule
                .as_deref()
                .unwrap_or(&[]),
        }
    }
}
