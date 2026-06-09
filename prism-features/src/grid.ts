import {WorldRegionNameStateData} from "./types";

export function build_grid_features(region_names: WorldRegionNameStateData[]): unknown[] {
    const features: unknown[] = [];

    const region_count = 5;
    const region_size_chunks = 80;
    const chunk_size = 96;
    const min_region = 0;
    const max_region = 4;
    const min_chunk = min_region * region_size_chunks;
    const max_chunk = (max_region + 1) * region_size_chunks;

    const grid_lines: number[][][] = [];
    for (let z = min_chunk + 1; z < max_chunk; z++) {
        grid_lines.push([
            [min_chunk * chunk_size, z * chunk_size],
            [max_chunk * chunk_size, z * chunk_size],
        ]);
    }
    for (let x = min_chunk + 1; x < max_chunk; x++) {
        grid_lines.push([
            [x * chunk_size, min_chunk * chunk_size],
            [x * chunk_size, max_chunk * chunk_size],
        ]);
    }

    features.push({
        type: "Feature",
        properties: {noPan: 1, color: "#737070", weight: 0.4, opacity: 1},
        geometry: {type: "MultiLineString", coordinates: grid_lines},
    });

    const region_borders: number[][][] = [];
    for (let rz = min_region; rz <= max_region + 1; rz++) {
        const z = rz * region_size_chunks * chunk_size;
        region_borders.push([
            [min_chunk * chunk_size, z],
            [max_chunk * chunk_size, z],
        ]);
    }
    for (let rx = min_region; rx <= max_region + 1; rx++) {
        const x = rx * region_size_chunks * chunk_size;
        region_borders.push([
            [x, min_chunk * chunk_size],
            [x, max_chunk * chunk_size],
        ]);
    }

    features.push({
        type: "Feature",
        properties: {noPan: 1, color: "#000000", weight: 2, opacity: 1},
        geometry: {type: "MultiLineString", coordinates: region_borders},
    });

    for (let rz = min_region; rz <= max_region; rz++) {
        for (let rx = min_region; rx <= max_region; rx++) {
            const region_idx = rz * region_count + rx + 1;
            const region = region_names.find((row) => row.id === region_idx);
            if (!region) continue;

            features.push({
                type: "Feature",
                properties: {type: "tooltip", noPan: 1, popupText: region.player_facing_name},
                geometry: {
                    type: "Point",
                    coordinates: [
                        (rx * region_size_chunks + region_size_chunks / 2) * chunk_size,
                        (rz * region_size_chunks + region_size_chunks / 2) * chunk_size,
                    ],
                },
            });
        }
    }

    return features;
}
