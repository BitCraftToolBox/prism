//! Top-level config loaded from `config.toml` with env-var overrides.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub upstream: UpstreamConfig,
    pub relay: RelayConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub pipelines: PipelinesConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    #[serde(default = "default_upstream_host")]
    pub host: String,
    /// Default token used for any region without an explicit token. Optional;
    /// if unset, only regions with their own token will be connected to.
    #[serde(default)]
    pub default_token: Option<String>,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    pub uri: String,
    pub module: String,
    pub token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PipelinesConfig {
    #[serde(default = "default_true")]
    pub resources: bool,
    #[serde(default = "default_true")]
    pub enemies: bool,
    #[serde(default = "default_true")]
    pub players: bool,
}

fn default_true() -> bool {
    true
}
fn default_upstream_host() -> String {
    "https://bitcraft-early-access.spacetimedb.com".into()
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
        if let Ok(t) = std::env::var("PRISM_RELAY_TOKEN") {
            self.relay.token = t;
        }
        if let Ok(u) = std::env::var("DATABASE_URL") {
            self.database.url = u;
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
        Ok(())
    }

    /// Token to use for a given region (per-region override → global default).
    pub fn token_for<'a>(&'a self, region: &'a RegionConfig) -> Option<&'a str> {
        region
            .token
            .as_deref()
            .or(self.upstream.default_token.as_deref())
    }
}
