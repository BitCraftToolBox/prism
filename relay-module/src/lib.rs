//! Prism relay SpacetimeDB module.
//!
//! Stores the current state (resources, enemies, players) for each BitCraft
//! region in a schema optimized for client-app subscriptions: indexed by
//! resource / enemy / region id rather than chunk index. Only the configured
//! relay identity (set once via [`reducers::init_relay`]) may write.

mod http;
mod reducers;
mod tables;
