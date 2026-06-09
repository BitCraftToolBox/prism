import * as fs from "node:fs";
import * as path from "node:path";
import {load_config} from "./config";
import {add_feature, create_outputs, make_claim_extras} from "./features";
import {fetch_global_data} from "./global";
import {build_grid_features} from "./grid";
import {load_region_data} from "./io";
import {build_watchtower_territories} from "./watchtower";

function write_json(output_dir: string, name: string, value: unknown): void {
    fs.writeFileSync(path.join(output_dir, `${name}.geojson`), JSON.stringify(value));
}

export async function main(args: string[] = process.argv.slice(2)): Promise<void> {
    const config = load_config(args);
    fs.mkdirSync(config.output_dir, {recursive: true});

    const region_data = load_region_data(config.input_dir);
    const global_data = await fetch_global_data(config);

    const local_state_map = new Map(region_data.claim_local_state.map((row) => [row.entity_id, row]));
    const claim_extras = make_claim_extras(region_data);
    const territories = build_watchtower_territories(region_data.claim_state, local_state_map, global_data);

    const outputs = create_outputs();

    for (const claim_state of region_data.claim_state) {
        const local_state = local_state_map.get(claim_state.entity_id);
        if (!local_state) continue;
        add_feature(outputs, claim_state, local_state, territories, region_data.hexite_timers, claim_extras);
    }

    outputs.grids = build_grid_features(region_data.world_region_name_state);

    write_json(config.output_dir, "caves", outputs.caves);
    write_json(config.output_dir, "trees", outputs.trees);
    write_json(config.output_dir, "ruined", outputs.ruined);
    write_json(config.output_dir, "temples", outputs.temples);
    write_json(config.output_dir, "dungeons", outputs.dungeons);
    write_json(config.output_dir, "towers", outputs.towers);
    write_json(config.output_dir, "grids", {type: "FeatureCollection", features: outputs.grids});
    write_json(config.output_dir, "claims", {type: "FeatureCollection", features: outputs.claims});
}

