import {EmpireState} from "../bindings_global/src";
import {ClaimLocalStateData, ClaimStateData, get_some_location, GlobalData} from "./types";
import {format_template_args} from "./utils";

export interface ChunkGroup {
    chunks: { chunk_x: number; chunk_z: number; chunk_index: bigint }[];
}

export interface WatchtowerTerritory {
    entity_id: bigint;
    location: { x: number; z: number };
    name: string;
    owner_id: bigint;
    owner_name: string;
    chunk_indices: bigint[];
    chunk_groups: ChunkGroup[];
    total_chunks: number;
    color: string;
    outline_color: string;
}

export function chunk_index_to_xz(chunk_index: bigint): { chunk_x: number; chunk_z: number } {
    const base = chunk_index - BigInt(1);
    return {
        chunk_x: Number(base % BigInt(1000)),
        chunk_z: Number(base / BigInt(1000)),
    };
}

function chunk_xz_to_tile_coords(chunk_x: number, chunk_z: number): { x: number; z: number } {
    return {x: chunk_x * 96, z: chunk_z * 96};
}

function argb_to_hex(argb: bigint | number): string {
    const num = typeof argb === "bigint" ? Number(argb) : argb;
    const r = (num >> 16) & 0xff;
    const g = (num >> 8) & 0xff;
    const b = num & 0xff;
    return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${b.toString(16).padStart(2, "0")}`;
}

function create_chunk_group_outline(chunk_group: ChunkGroup): number[][][] {
    if (chunk_group.chunks.length === 0) return [];

    const chunk_set = new Set(chunk_group.chunks.map((c) => `${c.chunk_x},${c.chunk_z}`));
    const edges = new Map<string, { x0: number; z0: number; x1: number; z1: number }>();

    for (const chunk of chunk_group.chunks) {
        const {x: x0, z: z0} = chunk_xz_to_tile_coords(chunk.chunk_x, chunk.chunk_z);
        const {x: x1, z: z1} = chunk_xz_to_tile_coords(chunk.chunk_x + 1, chunk.chunk_z + 1);

        if (!chunk_set.has(`${chunk.chunk_x},${chunk.chunk_z + 1}`)) {
            edges.set(`${x0},${z1}-${x1},${z1}`, {x0, z0: z1, x1, z1});
        }
        if (!chunk_set.has(`${chunk.chunk_x},${chunk.chunk_z - 1}`)) {
            edges.set(`${x0},${z0}-${x1},${z0}`, {x0, z0, x1, z1: z0});
        }
        if (!chunk_set.has(`${chunk.chunk_x + 1},${chunk.chunk_z}`)) {
            edges.set(`${x1},${z0}-${x1},${z1}`, {x0: x1, z0, x1, z1});
        }
        if (!chunk_set.has(`${chunk.chunk_x - 1},${chunk.chunk_z}`)) {
            edges.set(`${x0},${z0}-${x0},${z1}`, {x0, z0, x1: x0, z1});
        }
    }

    const edge_list = Array.from(edges.values());
    const loops: number[][][] = [];
    const used = new Set<number>();

    while (used.size < edge_list.length) {
        const start_idx = edge_list.findIndex((_edge, idx) => !used.has(idx));
        if (start_idx < 0) break;

        const loop: number[][] = [];
        const start = edge_list[start_idx];
        used.add(start_idx);
        loop.push([start.x0, start.z0], [start.x1, start.z1]);

        let guard = edge_list.length;
        while (guard-- > 0) {
            const [last_x, last_z] = loop[loop.length - 1];
            let found = false;

            for (let i = 0; i < edge_list.length; i++) {
                if (used.has(i)) continue;
                const edge = edge_list[i];

                if (edge.x0 === last_x && edge.z0 === last_z) {
                    loop.push([edge.x1, edge.z1]);
                    used.add(i);
                    found = true;
                    break;
                }

                if (edge.x1 === last_x && edge.z1 === last_z) {
                    loop.push([edge.x0, edge.z0]);
                    used.add(i);
                    found = true;
                    break;
                }
            }

            if (!found) break;
        }

        if (loop.length > 0) {
            loop.push(loop[0]);
            loops.push(loop);
        }
    }

    if (loops.length <= 1) {
        return loops;
    }

    let outer_idx = 0;
    let max_perimeter = 0;

    for (let i = 0; i < loops.length; i++) {
        let perimeter = 0;
        for (let j = 0; j < loops[i].length - 1; j++) {
            const dx = loops[i][j + 1][0] - loops[i][j][0];
            const dz = loops[i][j + 1][1] - loops[i][j][1];
            perimeter += Math.sqrt(dx * dx + dz * dz);
        }
        if (perimeter > max_perimeter) {
            max_perimeter = perimeter;
            outer_idx = i;
        }
    }

    const ordered = [loops[outer_idx]];
    for (let i = 0; i < loops.length; i++) {
        if (i !== outer_idx) ordered.push(loops[i]);
    }

    return ordered;
}

export function group_contiguous_chunk_indices(chunk_indices: bigint[]): ChunkGroup[] {
    const coords = chunk_indices.map((idx) => ({...chunk_index_to_xz(idx), chunk_index: idx}));
    const chunk_set = new Set(coords.map((c) => `${c.chunk_x},${c.chunk_z}`));
    const visited = new Set<string>();
    const groups: ChunkGroup[] = [];

    const lookup = new Map<string, { chunk_x: number; chunk_z: number; chunk_index: bigint }>();
    for (const item of coords) lookup.set(`${item.chunk_x},${item.chunk_z}`, item);

    function visit(x: number, z: number, group: ChunkGroup): void {
        const key = `${x},${z}`;
        if (visited.has(key) || !chunk_set.has(key)) return;
        visited.add(key);

        const found = lookup.get(key);
        if (found) group.chunks.push(found);

        visit(x - 1, z, group);
        visit(x + 1, z, group);
        visit(x, z - 1, group);
        visit(x, z + 1, group);
    }

    for (const item of coords) {
        const key = `${item.chunk_x},${item.chunk_z}`;
        if (visited.has(key)) continue;
        const group: ChunkGroup = {chunks: []};
        visit(item.chunk_x, item.chunk_z, group);
        if (group.chunks.length > 0) groups.push(group);
    }

    return groups;
}

function resolve_empire_colors(global_data: GlobalData, empire: EmpireState | undefined): { fill: string; outline: string } {
    if (!empire) return {fill: "#808080", outline: "#000000"};

    const emblem = global_data.empire_emblem_state.find((state) => state.entityId === empire.entityId);
    if (!emblem) return {fill: "#808080", outline: "#000000"};

    const fill_desc = global_data.empire_color_desc.find((row) => row.id === emblem.color2Id);
    const outline_desc = global_data.empire_color_desc.find((row) => row.id === emblem.color1Id);

    return {
        fill: fill_desc ? argb_to_hex(fill_desc.colorArgb) : "#808080",
        outline: outline_desc ? argb_to_hex(outline_desc.colorArgb) : "#000000",
    };
}

const PALETTE_EMPIRE_ID = BigInt(131);

// Empires listed here ignore their emblem colors; their territories are colored from this palette instead
const TERRITORY_COLOR_PALETTE = [
    "#0000ff",
    "#ff0000",
    "#00ff00",
    "#c71585",
    "#00ffff",
    "#808000",
    "#1e90ff",
];

function territory_chunk_key(chunk_x: number, chunk_z: number): string {
    return `${chunk_x},${chunk_z}`;
}

// True if any chunk in a is edge-adjacent to any chunk in b
function are_territories_adjacent(a: WatchtowerTerritory, b: WatchtowerTerritory): boolean {
    const chunk_set_a = new Set(a.chunk_groups.flatMap((g) => g.chunks.map((c) => territory_chunk_key(c.chunk_x, c.chunk_z))));
    for (const group of b.chunk_groups) {
        for (const chunk of group.chunks) {
            for (const [dx, dz] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) {
                if (chunk_set_a.has(territory_chunk_key(chunk.chunk_x + dx, chunk.chunk_z + dz))) return true;
            }
        }
    }
    return false;
}

// True if the watchtower location of a is within distance_chunks (in chunks) of any chunk in b
function are_territories_nearby(a: WatchtowerTerritory, b: WatchtowerTerritory, distance_chunks: number): boolean {
    const dist_sq = distance_chunks * distance_chunks;
    const chunk_ax = Math.floor(a.location.x / 96);
    const chunk_az = Math.floor(a.location.z / 96);
    for (const group of b.chunk_groups) {
        for (const chunk of group.chunks) {
            const dx = chunk_ax - chunk.chunk_x;
            const dz = chunk_az - chunk.chunk_z;
            if (dx * dx + dz * dz <= dist_sq) return true;
        }
    }
    return false;
}

// Greedy-color territories so that adjacent/nearby territories get different palette colors
function assign_territory_colors(territories: WatchtowerTerritory[], palette: string[], nearby_distance = 10) {
    const n = territories.length;
    const adj = Array.from({length: n}, () => new Set<number>());
    for (let i = 0; i < n; ++i) {
        for (let j = i + 1; j < n; ++j) {
            if (are_territories_adjacent(territories[i], territories[j]) ||
                are_territories_nearby(territories[i], territories[j], nearby_distance) ||
                are_territories_nearby(territories[j], territories[i], nearby_distance)) {
                adj[i].add(j);
                adj[j].add(i);
            }
        }
    }

    const colors: number[] = Array(n).fill(-1);
    for (let i = 0; i < n; ++i) {
        const used = new Set(Array.from(adj[i]).map((j) => colors[j]));
        for (let c = 0; c < palette.length; ++c) {
            if (!used.has(c)) {
                colors[i] = c;
                break;
            }
        }
        if (colors[i] === -1) {
            console.log(`Warning: not enough colors for territory ${territories[i].name}, assigning default color`);
            colors[i] = 0;
        }
        territories[i].color = palette[colors[i]];
    }
}

export function build_watchtower_territories(
    claim_states: ClaimStateData[],
    local_state_map: Map<bigint, ClaimLocalStateData>,
    global_data: GlobalData,
): WatchtowerTerritory[] {
    const watchtower_chunks = new Map<bigint, bigint[]>();
    const watchtower_empires = new Map<bigint, EmpireState>();

    for (const row of global_data.empire_chunk_state) {
        const current = watchtower_chunks.get(row.watchtowerEntityId) ?? [];
        current.push(row.chunkIndex);
        watchtower_chunks.set(row.watchtowerEntityId, current);

        if (!watchtower_empires.has(row.watchtowerEntityId)) {
            const empire = global_data.empire_state.find((state) => state.entityId === row.empireEntityId);
            if (empire) watchtower_empires.set(row.watchtowerEntityId, empire);
        }
    }

    const territories: WatchtowerTerritory[] = [];

    for (const claim_state of claim_states) {
        const local_state = local_state_map.get(claim_state.entity_id);
        if (!local_state || local_state.building_description_id !== 90000) continue;

        const location = get_some_location(local_state.location);
        if (!location) continue;

        const chunk_indices = watchtower_chunks.get(claim_state.owner_building_entity_id) ?? [];
        const chunk_groups = group_contiguous_chunk_indices(chunk_indices);
        const empire = watchtower_empires.get(claim_state.owner_building_entity_id);
        const colors = resolve_empire_colors(global_data, empire);

        territories.push({
            entity_id: claim_state.owner_building_entity_id,
            location: {x: location.x, z: location.z},
            name: format_template_args(claim_state.name),
            owner_id: empire?.entityId ?? BigInt(0),
            owner_name: empire?.name ?? "Unknown",
            chunk_indices,
            chunk_groups,
            total_chunks: chunk_indices.length,
            color: colors.fill,
            outline_color: colors.outline,
        });
    }

    assign_territory_colors(territories.filter((t) => t.owner_id === PALETTE_EMPIRE_ID), TERRITORY_COLOR_PALETTE);

    return territories;
}

export function make_tower_feature(
    claim_state: ClaimStateData,
    local_state: ClaimLocalStateData,
    territories: WatchtowerTerritory[],
): {type: "FeatureCollection", features: any[]} | null {
    const location = get_some_location(local_state.location);
    if (!location) return null;

    const territory = territories.find((t) => t.entity_id === claim_state.owner_building_entity_id);
    const towerEntityId = String(claim_state.owner_building_entity_id);
    const props = {
        towerEntityId,
        name: format_template_args(claim_state.name),
        owner: territory?.owner_name ?? null,
        ownerId: territory ? String(territory.owner_id) : null,
        chunkCount: territory?.total_chunks,
        fillColor: territory?.color,
        outlineColor: territory?.outline_color,
    };

    if (!territory || territory.chunk_indices.length === 0) {
        return {
            type: "FeatureCollection",
            features: [{type: "Feature", properties: props, geometry: {type: "Point", coordinates: [location.x, location.z]}}],
        };
    }

    const polygons: number[][][] = territory.chunk_indices.map((idx) => {
        const {chunk_x, chunk_z} = chunk_index_to_xz(idx);
        const {x: x0, z: z0} = chunk_xz_to_tile_coords(chunk_x, chunk_z);
        const {x: x1, z: z1} = chunk_xz_to_tile_coords(chunk_x + 1, chunk_z + 1);
        return [[x0, z0], [x1, z0], [x1, z1], [x0, z1], [x0, z0]];
    });

    const outlines = territory.chunk_groups
        .map((group) => create_chunk_group_outline(group))
        .filter((outline) => outline.length > 0);

    return {
        type: "FeatureCollection",
        features: [
            {
                type: "Feature",
                properties: {
                    ...props,
                    featureKind: "tower-chunks",
                    fillOpacity: 0.0,
                    fillColor: territory.color,
                    color: "#7f7f7f",
                    weight: 0.5,
                    pointCoords: [location.z, location.x],
                },
                geometry: {type: "MultiPolygon", coordinates: polygons.map((ring) => [ring])},
            },
            {
                type: "Feature",
                properties: {
                    featureKind: "tower-outline",
                    fillOpacity: 0.6,
                    color: territory.outline_color,
                    weight: 1,
                    pointCoords: [location.z, location.x],
                    ...props,
                },
                geometry: {type: "MultiPolygon", coordinates: outlines},
            },
            {
                type: "Feature",
                properties: {...props, featureKind: "tower-marker"},
                geometry: {type: "Point", coordinates: [location.x, location.z]}
            },
        ],
    };
}
