//! Resource heatmap renderer — reads the global resource description JSON,
//! then for each resource id in turn subscribes to that resource's
//! `resource_location` rows on the relay module, snapshots them from the
//! client cache, and writes a HexMaps-compatible heatmap `points` array.
//!
//! Output: `{output_dir}/resources/heatmaps/{resource_id}.json`

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path as FsPath;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use metrics::{gauge, histogram};
use relay_bindings::{
    DbConnection, ErrorContext, ResourceLocation, ResourceLocationTableAccess,
    SubscriptionEventContext,
};
use relay_sdk::{DbContext, SubscriptionHandle, Table};
use serde::{Deserialize, Serialize};

use crate::config::RelayConfig;
use crate::tile_generator::check_canceled;

/// Resources with more locations than this are pre-aggregated into weighted
/// clusters instead of being emitted one point per location.
const RAW_POINT_LIMIT: usize = 20_000;

const WORLD_MAX_COORD: i32 = 12_800;

/// How long to wait for a single resource's subscription to apply before
/// giving up.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct ResourceDesc {
    id: i32,
}

#[derive(Debug, Serialize)]
struct HeatPoint {
    x: i32,
    y: i32,
    intensity: f32,
}

/// Load resource ids from `input_dir/global/resource_desc.json`, connect to
/// the relay module, and write one heatmap-points JSON file per resource id
/// into `tiles_dir`.
pub fn render(
    input_dir: &FsPath,
    relay: Option<&RelayConfig>,
    tiles_dir: &FsPath,
    canceled: &AtomicBool,
) -> Result<()> {
    let relay = relay.context("[resources] no [relay] section configured")?;

    std::fs::create_dir_all(tiles_dir)
        .with_context(|| format!("Failed to create {}", tiles_dir.display()))?;

    let resource_ids = load_resource_ids(input_dir)?;
    log::info!("[resources] {} resource ids to process", resource_ids.len());

    let relay_conn = connect(relay)?;

    let result = (|| -> Result<()> {
        let mut total_bytes: u64 = 0;
        for resource_id in &resource_ids {
            check_canceled(canceled)?;

            let locations = subscribe_and_collect(&relay_conn.conn, *resource_id, canceled)?;
            log::debug!(
                "[resources] resource {}: {} locations",
                resource_id,
                locations.len()
            );

            let points = build_points(&locations);

            let out_path = tiles_dir.join(format!("{}.json", resource_id));
            let f = File::create(&out_path)
                .with_context(|| format!("Failed to create {}", out_path.display()))?;
            serde_json::to_writer(std::io::BufWriter::new(f), &points)
                .with_context(|| format!("Failed to write {}", out_path.display()))?;

            total_bytes += out_path.metadata().map(|m| m.len()).unwrap_or(0);
            // No "zoom" axis here (unlike tile_generator's per-zoom bytes),
            // so this reports the whole task's running total under the same
            // metric name/`task` label the tile renderers use.
            gauge!("cartographer_tile_bytes_total", "task" => "resources").set(total_bytes as f64);
        }
        Ok(())
    })();

    if let Err(e) = relay_conn.conn.disconnect() {
        log::warn!("[resources] relay disconnect error: {:?}", e);
    }
    let _ = relay_conn.pump.join();

    result?;

    log::info!("[resources] done");
    Ok(())
}

fn load_resource_ids(input_dir: &FsPath) -> Result<Vec<i32>> {
    let desc_path = input_dir.join("global").join("resource_desc.json");
    let f = File::open(&desc_path)
        .with_context(|| format!("Failed to open {}", desc_path.display()))?;
    let descs: Vec<ResourceDesc> = serde_json::from_reader(BufReader::new(f))
        .with_context(|| format!("Failed to parse {}", desc_path.display()))?;
    Ok(descs.into_iter().map(|d| d.id).collect())
}

/// A connected relay client. Holds the connection and the pump thread handle
/// so the thread can be joined on clean shutdown.
struct RelayConn {
    conn: DbConnection,
    pump: std::thread::JoinHandle<()>,
}

fn connect(relay: &RelayConfig) -> Result<RelayConn> {
    let token = relay
        .token
        .as_deref();

    let conn = DbConnection::builder()
        .with_uri(&relay.uri)
        .with_database_name(&relay.module)
        .with_token(token)
        .on_connect(|_ctx, _id, _tok| {
            log::info!("[resources] connected to relay");
        })
        .on_disconnect(|_ctx, err| match err {
            Some(e) => log::warn!("[resources] relay disconnected: {:?}", e),
            None => log::info!("[resources] relay disconnected"),
        })
        .build()
        .map_err(|e| anyhow!("[resources] relay connect failed: {:?}", e))?;

    let pump = conn.run_threaded();

    Ok(RelayConn { conn, pump })
}

/// Subscribe to `resource_location` rows for a single resource id, wait for
/// the subscription to apply, snapshot the matching rows from the client
/// cache, then unsubscribe before returning so the caller can move on to the
/// next resource id.
fn subscribe_and_collect(
    conn: &DbConnection,
    resource_id: i32,
    canceled: &AtomicBool,
) -> Result<Vec<ResourceLocation>> {
    let query = format!(
        "SELECT * FROM resource_location WHERE resource_id = {};",
        resource_id
    );

    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    let tx_err = tx.clone();

    let subscribe_start = Instant::now();
    let handle = conn
        .subscription_builder()
        .on_applied(move |_ctx: &SubscriptionEventContext| {
            let _ = tx.send(Ok(()));
        })
        .on_error(move |_ctx: &ErrorContext, e: relay_sdk::Error| {
            let _ = tx_err.send(Err(format!("{:?}", e)));
        })
        .subscribe(vec![query]);

    let deadline = subscribe_start + SUBSCRIBE_TIMEOUT;
    loop {
        check_canceled(canceled)?;
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(Ok(())) => break,
            Ok(Err(e)) => bail!(
                "[resources] subscription error for resource {}: {}",
                resource_id,
                e
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if Instant::now() >= deadline {
                    bail!(
                        "[resources] timed out waiting for subscription to apply for resource {}",
                        resource_id
                    );
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!(
                "[resources] subscription channel closed unexpectedly for resource {}",
                resource_id
            ),
        }
    }
    histogram!("cartographer_resources_subscribe_duration_seconds", "task" => "resources")
        .record(subscribe_start.elapsed().as_secs_f64());

    // The query already filters server-side, but filter defensively here too
    // in case rows from a still-draining previous subscription (for a
    // different resource id) haven't been removed from the cache yet.
    let rows: Vec<ResourceLocation> = conn
        .db
        .resource_location()
        .iter()
        .filter(|r| r.resource_id == resource_id)
        .collect();

    handle.unsubscribe().map_err(|e| {
        anyhow!(
            "[resources] failed to unsubscribe resource {}: {:?}",
            resource_id,
            e
        )
    })?;

    Ok(rows)
}

fn to_hexmap_coord(x: i32, z: i32) -> (i32, i32) {
    (x / 3, WORLD_MAX_COORD - z / 3)
}

/// Below `RAW_POINT_LIMIT`, emit one point per location at constant
/// intensity. Above it, aggregate into a grid of weighted clusters — start
/// at minimap resolution and keep doubling the cell size until the cluster
/// count fits the budget.
fn build_points(locations: &[ResourceLocation]) -> Vec<HeatPoint> {
    if locations.len() <= RAW_POINT_LIMIT {
        return locations
            .iter()
            .map(|loc| {
                let (mx, my) = to_hexmap_coord(loc.x, loc.z);
                HeatPoint {
                    x: mx,
                    y: my,
                    intensity: 1.0,
                }
            })
            .collect();
    }

    let mut cells: HashMap<(i32, i32), u32> = HashMap::new();
    for loc in locations {
        let key = to_hexmap_coord(loc.x, loc.z);
        *cells.entry(key).or_insert(0) += 1;
    }

    let mut cell_size: i32 = 1;
    loop {
        // (sum_x, sum_y, count) weighted by how many source locations fall
        // into each finer cell, so the emitted point sits at the true
        // centroid of its members
        let mut buckets: HashMap<(i32, i32), (i64, i64, u32)> = HashMap::new();
        for (&(x, y), &count) in &cells {
            let key = (x.div_euclid(cell_size), y.div_euclid(cell_size));
            let entry = buckets.entry(key).or_insert((0, 0, 0));
            entry.0 += x as i64 * count as i64;
            entry.1 += y as i64 * count as i64;
            entry.2 += count;
        }

        if buckets.len() <= RAW_POINT_LIMIT || cell_size >= WORLD_MAX_COORD {
            return buckets
                .into_values()
                .map(|(sum_x, sum_y, count)| HeatPoint {
                    x: (sum_x / count as i64) as i32,
                    y: (sum_y / count as i64) as i32,
                    intensity: count as f32,
                })
                .collect();
        }

        cell_size *= 2;
    }
}
