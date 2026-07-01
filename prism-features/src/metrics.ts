import * as http from 'node:http';
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const globalFetchDuration = new Histogram({
    name: 'features_global_fetch_duration_seconds',
    help: 'SpacetimeDB global data subscription duration in seconds',
    registers: [registry],
});

export const jsonParseDuration = new Histogram({
    name: 'features_json_parse_duration_seconds',
    help: 'Region JSON file read and parse duration in seconds',
    registers: [registry],
});

export const jsonRowsTotal = new Counter({
    name: 'features_json_rows_total',
    help: 'Number of rows loaded from region JSON files',
    labelNames: ['table'] as const,
    registers: [registry],
});

export const geojsonGenerationDuration = new Histogram({
    name: 'features_geojson_generation_duration_seconds',
    help: 'GeoJSON feature generation duration in seconds',
    labelNames: ['layer'] as const,
    registers: [registry],
});

export const geojsonFeatureCount = new Gauge({
    name: 'features_geojson_feature_count',
    help: 'Number of features written per GeoJSON layer',
    labelNames: ['layer'] as const,
    registers: [registry],
});

export const runDuration = new Histogram({
    name: 'features_run_duration_seconds',
    help: 'Total wall time for one full feature generation run in seconds',
    registers: [registry],
});

export function startMetricsServer(port: number = 9090): void {
    const server = http.createServer(async (req, res) => {
        if (req.url === '/metrics') {
            try {
                res.setHeader('Content-Type', registry.contentType);
                res.end(await registry.metrics());
            } catch (e) {
                res.writeHead(500);
                res.end(String(e));
            }
        } else {
            res.writeHead(404);
            res.end();
        }
    });
    server.listen(port, () => {
        console.log(`Metrics server listening on :${port}`);
    });
}
