//! Helpers for extracting and serializing rows from a [`DbUpdate`] by table name.
//!
//! [`SupportedTable`] is the authoritative list of tables that can be dumped.
//! `has_inserts` and `extract_rows_json` both match exhaustively on it, so the
//! compiler ensures they stay in sync.  To add a new table: add a variant to
//! the enum, add its string name to `as_str`, add it to `ALL`, and add a match
//! arm to both functions.
//!
//! Serialisation uses [`SerdeWrapper`] from `spacetimedb_sats` to bridge the
//! SATS `Serialize` trait (implemented by all generated row types) to
//! `serde::Serialize` so that `serde_json` can consume the rows.

use serde_json::Value;
use upstream_bindings::region::DbUpdate;
use upstream_bindings::sdk::__codegen::__sats;

// Convenience alias for the wrapper that bridges SATS ↔ serde.
use __sats::serde::SerdeWrapper;

/// Every table that the dumper knows how to extract rows from.
///
/// Adding a table requires changes to:
/// - [`SupportedTable::as_str`]
/// - [`SupportedTable::ALL`]
/// - [`has_inserts`]
/// - [`extract_rows_json`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SupportedTable {
    BiomeDesc,
    PavingTileDesc,
    TerrainChunkState,
    PavedTileState,
    LocationState,
    WorldRegionState,
    ClaimState,
    ClaimLocalState,
    ClaimTechState,
    WaystoneState,
    BankState,
    MarketplaceState,
    WorldRegionNameState,
    GrowthState,
    ResourceState,
}

impl SupportedTable {
    /// Canonical table name as it appears in the upstream database.
    pub fn as_str(self) -> &'static str {
        match self {
            SupportedTable::BiomeDesc => "biome_desc",
            SupportedTable::PavingTileDesc => "paving_tile_desc",
            SupportedTable::TerrainChunkState => "terrain_chunk_state",
            SupportedTable::PavedTileState => "paved_tile_state",
            SupportedTable::LocationState => "location_state",
            SupportedTable::WorldRegionState => "world_region_state",
            SupportedTable::ClaimState => "claim_state",
            SupportedTable::ClaimLocalState => "claim_local_state",
            SupportedTable::ClaimTechState => "claim_tech_state",
            SupportedTable::WaystoneState => "waystone_state",
            SupportedTable::BankState => "bank_state",
            SupportedTable::MarketplaceState => "marketplace_state",
            SupportedTable::WorldRegionNameState => "world_region_name_state",
            SupportedTable::GrowthState => "growth_state",
            SupportedTable::ResourceState => "resource_state",
        }
    }

    /// All supported tables. Used for validation and documentation.
    pub const ALL: &'static [SupportedTable] = &[
        SupportedTable::BiomeDesc,
        SupportedTable::PavingTileDesc,
        SupportedTable::TerrainChunkState,
        SupportedTable::PavedTileState,
        SupportedTable::LocationState,
        SupportedTable::WorldRegionState,
        SupportedTable::ClaimState,
        SupportedTable::ClaimLocalState,
        SupportedTable::ClaimTechState,
        SupportedTable::WaystoneState,
        SupportedTable::BankState,
        SupportedTable::MarketplaceState,
        SupportedTable::WorldRegionNameState,
        SupportedTable::GrowthState,
        SupportedTable::ResourceState,
    ];

    /// Parse a table name string into a [`SupportedTable`], returning `None`
    /// if the name is not in the supported set.
    pub fn from_name(name: &str) -> Option<Self> {
        SupportedTable::ALL
            .iter()
            .copied()
            .find(|t| t.as_str() == name)
    }
}

/// Returns `true` if `update` contains at least one inserted row for `table`.
pub fn has_inserts(update: &DbUpdate, table: SupportedTable) -> bool {
    match table {
        SupportedTable::BiomeDesc => !update.biome_desc.inserts.is_empty(),
        SupportedTable::PavingTileDesc => !update.paving_tile_desc.inserts.is_empty(),
        SupportedTable::TerrainChunkState => !update.terrain_chunk_state.inserts.is_empty(),
        SupportedTable::PavedTileState => !update.paved_tile_state.inserts.is_empty(),
        SupportedTable::LocationState => !update.location_state.inserts.is_empty(),
        SupportedTable::WorldRegionState => !update.world_region_state.inserts.is_empty(),
        SupportedTable::ClaimState => !update.claim_state.inserts.is_empty(),
        SupportedTable::ClaimLocalState => !update.claim_local_state.inserts.is_empty(),
        SupportedTable::ClaimTechState => !update.claim_tech_state.inserts.is_empty(),
        SupportedTable::WaystoneState => !update.waystone_state.inserts.is_empty(),
        SupportedTable::BankState => !update.bank_state.inserts.is_empty(),
        SupportedTable::MarketplaceState => !update.marketplace_state.inserts.is_empty(),
        SupportedTable::WorldRegionNameState => !update.world_region_name_state.inserts.is_empty(),
        SupportedTable::GrowthState => !update.growth_state.inserts.is_empty(),
        SupportedTable::ResourceState => !update.resource_state.inserts.is_empty(),
    }
}

/// Extracts all inserted rows for `table` from `update` and serializes them
/// as a `Vec<serde_json::Value>`.
pub fn extract_rows_json(update: &DbUpdate, table: SupportedTable) -> Vec<Value> {
    match table {
        SupportedTable::BiomeDesc => serialize(&update.biome_desc),
        SupportedTable::PavingTileDesc => serialize(&update.paving_tile_desc),
        SupportedTable::TerrainChunkState => serialize(&update.terrain_chunk_state),
        SupportedTable::PavedTileState => serialize(&update.paved_tile_state),
        SupportedTable::LocationState => serialize(&update.location_state),
        SupportedTable::WorldRegionState => serialize(&update.world_region_state),
        SupportedTable::ClaimState => serialize(&update.claim_state),
        SupportedTable::ClaimLocalState => serialize(&update.claim_local_state),
        SupportedTable::ClaimTechState => serialize(&update.claim_tech_state),
        SupportedTable::WaystoneState => serialize(&update.waystone_state),
        SupportedTable::BankState => serialize(&update.bank_state),
        SupportedTable::MarketplaceState => serialize(&update.marketplace_state),
        SupportedTable::WorldRegionNameState => serialize(&update.world_region_name_state),
        SupportedTable::GrowthState => serialize(&update.growth_state),
        SupportedTable::ResourceState => serialize(&update.resource_state),
    }
}

fn serialize<Row>(tbl: &upstream_bindings::sdk::__codegen::TableUpdate<Row>) -> Vec<Value>
where
    Row: __sats::ser::Serialize + Clone,
{
    tbl.inserts
        .iter()
        .filter_map(|w| serde_json::to_value(SerdeWrapper(w.row.clone())).ok())
        .collect()
}
