import * as fs from "node:fs";
import * as path from "node:path";
import {load_config} from "./config";
import {add_feature, create_outputs, make_claim_extras} from "./features";
import {fetch_global_data} from "./global";
import {build_grid_features} from "./grid";
import {load_region_data} from "./io";
import {
    geojsonFeatureCount,
    geojsonGenerationDuration,
    globalFetchDuration,
    jsonParseDuration,
    dataRowsCount,
    runDuration,
} from "./metrics";
import {build_watchtower_territories} from "./watchtower";

function write_json(output_dir: string, name: string, value: unknown): void {
    fs.writeFileSync(path.join(output_dir, `${name}.geojson`), JSON.stringify(value));
}

function time<T>(fn: () => T): [T, number] {
    const start = performance.now();
    const result = fn();
    return [result, (performance.now() - start) / 1000];
}

async function timeAsync<T>(fn: () => Promise<T>): Promise<[T, number]> {
    const start = performance.now();
    const result = await fn();
    return [result, (performance.now() - start) / 1000];
}

export async function main(args: string[] = process.argv.slice(2)): Promise<void> {
    const runStart = performance.now();
    const config = load_config(args);
    fs.mkdirSync(config.output_dir, {recursive: true});

    const [region_data, parseSecs] = time(() => load_region_data(config.input_dir));
    jsonParseDuration.observe(parseSecs);
    dataRowsCount.labels('claim_state').set(region_data.claim_state.length);
    dataRowsCount.labels('growth_timers').set(region_data.growth_timers.length);

    const [global_data, fetchSecs] = await timeAsync(() => fetch_global_data(config));
    globalFetchDuration.observe(fetchSecs);
    dataRowsCount.labels('empire_state').set(global_data.empire_state.length);
    dataRowsCount.labels('empire_chunk_state').set(global_data.empire_chunk_state.length);

    const genTimer = geojsonGenerationDuration.startTimer({layer: 'all'});
    const local_state_map = new Map(region_data.claim_local_state.map((row) => [row.entity_id, row]));
    const claim_extras = make_claim_extras(region_data);
    const territories = build_watchtower_territories(region_data.claim_state, local_state_map, global_data);

    const outputs = create_outputs();

    for (const claim_state of region_data.claim_state) {
        const local_state = local_state_map.get(claim_state.entity_id);
        if (!local_state) continue;
        add_feature(outputs, claim_state, local_state, territories, region_data.growth_timers, claim_extras);
    }

    outputs.grids = build_grid_features(region_data.world_region_name_state);
    genTimer();

    for (const [name, arr] of Object.entries(outputs) as [string, unknown[]][]) {
        geojsonFeatureCount.labels(name).set(arr.length);
    }

    write_json(config.output_dir, "caves", outputs.caves);
    write_json(config.output_dir, "trees", outputs.trees);
    write_json(config.output_dir, "empireResources", outputs.empireResources);
    write_json(config.output_dir, "uncharted", outputs.uncharted);
    write_json(config.output_dir, "events", outputs.events);
    write_json(config.output_dir, "npcs", outputs.npcs);
    write_json(config.output_dir, "temples", outputs.temples);
    write_json(config.output_dir, "dungeons", outputs.dungeons);
    write_json(config.output_dir, "towers", {type: "FeatureCollection", features: outputs.towers});
    write_json(config.output_dir, "grids", {type: "FeatureCollection", features: outputs.grids});
    write_json(config.output_dir, "claims", {type: "FeatureCollection", features: outputs.claims});

    runDuration.observe((performance.now() - runStart) / 1000);
}

