import * as fs from "node:fs";
import * as path from "node:path";

interface FileConfig {
    input_dir?: string;
    output_dir?: string;
    upstream_hostname?: string;
    upstream_token?: string;
}

export interface RuntimeConfig {
    input_dir: string;
    output_dir: string;
    upstream_hostname: string;
    upstream_token: string;
    config_path: string;
}

function resolve_config_path(args: string[]): string {
    const idx = args.indexOf("--config");
    if (idx >= 0 && args[idx + 1]) {
        return path.resolve(args[idx + 1]);
    }

    if (args[0] && !args[0].startsWith("-")) {
        return path.resolve(args[0]);
    }

    return path.resolve("config.json");
}

export function load_config(args: string[]): RuntimeConfig {
    const config_path = resolve_config_path(args);
    if (!fs.existsSync(config_path)) {
        throw new Error(`Missing config file at ${config_path}`);
    }

    const parsed = JSON.parse(fs.readFileSync(config_path, "utf-8")) as FileConfig;
    const base_dir = path.dirname(config_path);

    const input_dir_raw = parsed.input_dir;
    const output_dir_raw = parsed.output_dir;
    const upstream_hostname = parsed.upstream_hostname ?? "https://bitcraft-early-access.spacetimedb.com";
    const upstream_token = process.env.PRISM_UPSTREAM_TOKEN ?? parsed.upstream_token;

    if (!input_dir_raw || !output_dir_raw) {
        throw new Error("config.json must define input_dir and output_dir");
    }

    if (!upstream_token) {
        throw new Error("Set upstream token in config.json or via PRISM_UPSTREAM_TOKEN env.");
    }

    return {
        input_dir: path.resolve(base_dir, input_dir_raw),
        output_dir: path.resolve(base_dir, output_dir_raw),
        upstream_hostname,
        upstream_token,
        config_path,
    };
}

