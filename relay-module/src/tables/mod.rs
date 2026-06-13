use spacetimedb::{Identity, table};

pub mod enemies;
pub mod players;
pub mod resources;

/// Single-row config table holding the identity of the authorized relay client.
#[table(accessor = relay_config)]
pub struct RelayConfig {
    #[primary_key]
    pub id: u8, // always 0
    pub identity: Identity,
}
