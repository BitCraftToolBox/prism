import * as fs from "node:fs";
import * as path from "node:path";
import {
  BankStateData,
  ClaimLocalStateData,
  ClaimStateData,
  ClaimTechStateData,
  MarketplaceStateData,
  RegionData,
  WaystoneStateData,
  WorldRegionNameStateData,
} from "./types";

function bigint_reviver(_key: string, value: unknown): unknown {
    if (typeof value === "string" && /^-?\d+n$/.test(value)) {
        return BigInt(value.slice(0, -1));
    }
    return value;
}

function parse_json_with_bigints<T>(raw: string): T {
    const with_bigint_markers = raw.replace(/:\s*(-?\d{16,})(?=\s*[,}\]])/g, ': "$1n"');
    return JSON.parse(with_bigint_markers, bigint_reviver) as T;
}

function read_json_file<T>(file_path: string, fallback: T): T {
    if (!fs.existsSync(file_path)) return fallback;
    const raw = fs.readFileSync(file_path, "utf-8");
    return parse_json_with_bigints<T>(raw);
}

function infer_region_id(region_dir: string): number {
    const base = path.basename(region_dir);
    const match = base.match(/bitcraft-live-(\d+)/);
    if (!match) return 0;
    return Number(match[1]);
}

function list_region_dirs(input_dir: string): string[] {
    const dirs = fs
        .readdirSync(input_dir, {withFileTypes: true})
        .filter((entry: fs.Dirent) => entry.isDirectory())
        .map((entry: fs.Dirent) => path.join(input_dir, entry.name));

    return dirs.filter((dir: string) => fs.existsSync(path.join(dir, "claim_state.json")));
}

export function load_region_data(input_dir: string): RegionData {
    const region_dirs = list_region_dirs(input_dir);
    if (region_dirs.length === 0) {
        throw new Error(`No dump regions found under ${input_dir}`);
    }

    const combined: RegionData = {
        claim_state: [],
        claim_local_state: [],
        world_region_name_state: [],
        bank_state: [],
        marketplace_state: [],
        waystone_state: [],
        claim_tech_state: [],
    };

    for (const region_dir of region_dirs) {
        const claim_state = read_json_file<ClaimStateData[]>(path.join(region_dir, "claim_state.json"), []);
        const claim_local_state = read_json_file<ClaimLocalStateData[]>(path.join(region_dir, "claim_local_state.json"), []);
        const claim_tech_state = read_json_file<ClaimTechStateData[]>(path.join(region_dir, "claim_tech_state.json"), []);
        const bank_state = read_json_file<BankStateData[]>(path.join(region_dir, "bank_state.json"), []);
        const marketplace_state = read_json_file<MarketplaceStateData[]>(path.join(region_dir, "marketplace_state.json"), []);
        const waystone_state = read_json_file<WaystoneStateData[]>(path.join(region_dir, "waystone_state.json"), []);

        const region_name_state = read_json_file<WorldRegionNameStateData[]>(
            path.join(region_dir, "world_region_name_state.json"),
            [],
        );

        const region_id = infer_region_id(region_dir);
        if (region_name_state[0]) {
            combined.world_region_name_state.push({
                id: region_id,
                player_facing_name: region_name_state[0].player_facing_name,
            });
        }

        combined.claim_state.push(...claim_state);
        combined.claim_local_state.push(...claim_local_state);
        combined.claim_tech_state.push(...claim_tech_state);
        combined.bank_state.push(...bank_state);
        combined.marketplace_state.push(...marketplace_state);
        combined.waystone_state.push(...waystone_state);
    }

    return combined;
}



