import {EmpireChunkState, EmpireColorDesc, EmpireEmblemState, EmpireState,} from "../bindings_global/src";

export interface TileLocation {
    dimension: number;
    x: number;
    z: number;
}

export interface JsonOptionSome<T> {
    some?: T;
    none?: unknown;
}

export interface ClaimStateData {
    entity_id: bigint;
    owner_building_entity_id: bigint;
    name: string;
}

export interface ClaimLocalStateData {
    entity_id: bigint;
    building_description_id: number;
    location: JsonOptionSome<TileLocation>;
}

export interface ClaimTechStateData {
    entity_id: bigint;
    learned: number[];
}

export interface WorldRegionNameStateData {
    id: number;
    player_facing_name: string;
}

export interface BankStateData {
    claim_entity_id: bigint;
}

export interface MarketplaceStateData {
    claim_entity_id: bigint;
}

export interface WaystoneStateData {
    claim_entity_id: bigint;
}

export interface RegionData {
    claim_state: ClaimStateData[];
    claim_local_state: ClaimLocalStateData[];
    world_region_name_state: WorldRegionNameStateData[];
    bank_state: BankStateData[];
    marketplace_state: MarketplaceStateData[];
    waystone_state: WaystoneStateData[];
    claim_tech_state: ClaimTechStateData[];
}

export interface GlobalData {
    empire_state: EmpireState[];
    empire_chunk_state: EmpireChunkState[];
    empire_color_desc: EmpireColorDesc[];
    empire_emblem_state: EmpireEmblemState[];
}

export function get_some_location(option: JsonOptionSome<TileLocation> | undefined): TileLocation | undefined {
    if (!option) return undefined;
    return option.some;
}

