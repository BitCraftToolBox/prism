//! Resource + herd heatmap renderer — reads the global resource/enemy
//! description JSON, then for each resource id (resp. enemy type) in turn
//! subscribes to its `resource_location` (resp. `herd_location`) rows on the
//! relay module, snapshots them from the client cache, and writes a
//! HexMaps-compatible heatmap `points` array.
//!
//! Output: `{output_dir}/heatmaps/resources/{resource_id}.json` and
//! `{output_dir}/heatmaps/herds/{enemy_type}.json`

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
    DbConnection, ErrorContext, HerdLocation, HerdLocationTableAccess, ResourceLocation,
    ResourceLocationTableAccess, SubscriptionEventContext,
};
use relay_sdk::{DbContext, SubscriptionHandle, Table};
use serde::{Deserialize, Serialize};

use crate::config::RelayConfig;
use crate::tile_generator::check_canceled;

/// Resources/herds with more locations than this are pre-aggregated into
/// weighted clusters instead of being emitted one point per location.
const RAW_POINT_LIMIT: usize = 20_000;

const WORLD_MAX_COORD: i32 = 12_800;

/// How long to wait for a single subscription to apply before giving up.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct ResourceDesc {
    id: i32,
}

#[derive(Debug, Deserialize)]
struct EnemyDesc {
    enemy_type: i32,
}

#[derive(Debug, Serialize)]
struct HeatPoint {
    x: i32,
    y: i32,
    intensity: f32,
}

/// Load resource ids from `input_dir/global/resource_desc.json` and enemy
/// types from `input_dir/global/enemy_desc.json`, connect to the relay
/// module, and write one heatmap-points JSON file per id into `tiles_dir`'s
/// `resources` and `herds` subfolders respectively.
pub fn render(
    input_dir: &FsPath,
    relay: Option<&RelayConfig>,
    tiles_dir: &FsPath,
    canceled: &AtomicBool,
) -> Result<()> {
    let relay = relay.context("[resources] no [relay] section configured")?;

    let resources_dir = tiles_dir.join("resources");
    let herds_dir = tiles_dir.join("herds");
    std::fs::create_dir_all(&resources_dir)
        .with_context(|| format!("Failed to create {}", resources_dir.display()))?;
    std::fs::create_dir_all(&herds_dir)
        .with_context(|| format!("Failed to create {}", herds_dir.display()))?;

    let resource_ids = load_resource_ids(input_dir)?;
    log::info!("[resources] {} resource ids to process", resource_ids.len());
    let enemy_types = load_enemy_types(input_dir)?;
    log::info!("[resources] {} enemy types to process", enemy_types.len());

    let relay_conn = connect(relay)?;

    let result = (|| -> Result<()> {
        let mut total_bytes: u64 = 0;
        for resource_id in &resource_ids {
            check_canceled(canceled)?;

            let locations =
                subscribe_and_collect_resources(&relay_conn.conn, *resource_id, canceled)?;
            log::debug!(
                "[resources] resource {}: {} locations",
                resource_id,
                locations.len()
            );

            let points = build_points(&locations.iter().map(|l| (l.x, l.z)).collect::<Vec<_>>());
            total_bytes += write_points(&resources_dir, *resource_id, &points)?;
            gauge!("cartographer_tile_bytes_total", "task" => "resources").set(total_bytes as f64);
        }

        for enemy_type in &enemy_types {
            check_canceled(canceled)?;

            let locations = subscribe_and_collect_herds(&relay_conn.conn, *enemy_type, canceled)?;
            log::debug!(
                "[resources] herd enemy_type {}: {} locations",
                enemy_type,
                locations.len()
            );

            let points = build_points(&locations.iter().map(|l| (l.x, l.z)).collect::<Vec<_>>());
            total_bytes += write_points(&herds_dir, *enemy_type, &points)?;
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

/// Serialize `points` to `{dir}/{id}.json`, returning the file's byte size.
fn write_points(dir: &FsPath, id: i32, points: &[HeatPoint]) -> Result<u64> {
    let out_path = dir.join(format!("{}.json", id));
    let f = File::create(&out_path)
        .with_context(|| format!("Failed to create {}", out_path.display()))?;
    serde_json::to_writer(std::io::BufWriter::new(f), points)
        .with_context(|| format!("Failed to write {}", out_path.display()))?;
    Ok(out_path.metadata().map(|m| m.len()).unwrap_or(0))
}

fn load_resource_ids(input_dir: &FsPath) -> Result<Vec<i32>> {
    let desc_path = input_dir.join("global").join("resource_desc.json");
    let f = File::open(&desc_path)
        .with_context(|| format!("Failed to open {}", desc_path.display()))?;
    let descs: Vec<ResourceDesc> = serde_json::from_reader(BufReader::new(f))
        .with_context(|| format!("Failed to parse {}", desc_path.display()))?;
    Ok(descs.into_iter().map(|d| d.id).collect())
}

fn load_enemy_types(input_dir: &FsPath) -> Result<Vec<i32>> {
    let desc_path = input_dir.join("global").join("enemy_desc.json");
    let f = File::open(&desc_path)
        .with_context(|| format!("Failed to open {}", desc_path.display()))?;
    let descs: Vec<EnemyDesc> = serde_json::from_reader(BufReader::new(f))
        .with_context(|| format!("Failed to parse {}", desc_path.display()))?;
    Ok(descs.into_iter().map(|d| d.enemy_type).collect())
}

/// A connected relay client. Holds the connection and the pump thread handle
/// so the thread can be joined on clean shutdown.
struct RelayConn {
    conn: DbConnection,
    pump: std::thread::JoinHandle<()>,
}

fn connect(relay: &RelayConfig) -> Result<RelayConn> {
    let token = relay.token.as_deref();

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

/// Block until a just-issued subscription applies (or times out / the run is
/// canceled). Shared polling loop for both per-id subscribe/collect helpers
/// below; each owns its `subscribe()` call since the returned handle type is
/// tied to the specific query/table and isn't object-safe to type-erase.
fn wait_for_applied(
    rx: &mpsc::Receiver<Result<(), String>>,
    label: &str,
    id: i32,
    canceled: &AtomicBool,
) -> Result<()> {
    let start = Instant::now();
    let deadline = start + SUBSCRIBE_TIMEOUT;
    loop {
        check_canceled(canceled)?;
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(Ok(())) => break,
            Ok(Err(e)) => bail!("[resources] subscription error for {} {}: {}", label, id, e),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if Instant::now() >= deadline {
                    bail!(
                        "[resources] timed out waiting for subscription to apply for {} {}",
                        label,
                        id
                    );
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!(
                "[resources] subscription channel closed unexpectedly for {} {}",
                label,
                id
            ),
        }
    }
    histogram!("cartographer_resources_subscribe_duration_seconds", "task" => label.to_owned())
        .record(start.elapsed().as_secs_f64());
    Ok(())
}

/// Subscribe to `resource_location` rows for a single resource id, wait for
/// the subscription to apply, snapshot the matching rows from the client
/// cache, then unsubscribe before returning so the caller can move on to the
/// next resource id.
fn subscribe_and_collect_resources(
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
    let handle = conn
        .subscription_builder()
        .on_applied(move |_ctx: &SubscriptionEventContext| {
            let _ = tx.send(Ok(()));
        })
        .on_error(move |_ctx: &ErrorContext, e: relay_sdk::Error| {
            let _ = tx_err.send(Err(format!("{:?}", e)));
        })
        .subscribe(vec![query]);

    wait_for_applied(&rx, "resource", resource_id, canceled)?;

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

/// Subscribe to `herd_location` rows for a single enemy type, wait for the
/// subscription to apply, snapshot the matching rows from the client cache,
/// then unsubscribe before returning so the caller can move on to the next
/// enemy type.
fn subscribe_and_collect_herds(
    conn: &DbConnection,
    enemy_type: i32,
    canceled: &AtomicBool,
) -> Result<Vec<HerdLocation>> {
    let query = format!(
        "SELECT * FROM herd_location WHERE enemy_type = {};",
        enemy_type
    );

    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    let tx_err = tx.clone();
    let handle = conn
        .subscription_builder()
        .on_applied(move |_ctx: &SubscriptionEventContext| {
            let _ = tx.send(Ok(()));
        })
        .on_error(move |_ctx: &ErrorContext, e: relay_sdk::Error| {
            let _ = tx_err.send(Err(format!("{:?}", e)));
        })
        .subscribe(vec![query]);

    wait_for_applied(&rx, "herd", enemy_type, canceled)?;

    // The query already filters server-side, but filter defensively here too
    // in case rows from a still-draining previous subscription (for a
    // different enemy type) haven't been removed from the cache yet.
    let rows: Vec<HerdLocation> = conn
        .db
        .herd_location()
        .iter()
        .filter(|r| r.enemy_type == enemy_type)
        .collect();

    handle.unsubscribe().map_err(|e| {
        anyhow!(
            "[resources] failed to unsubscribe herd enemy_type {}: {:?}",
            enemy_type,
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
fn build_points(locations: &[(i32, i32)]) -> Vec<HeatPoint> {
    if locations.len() <= RAW_POINT_LIMIT {
        return locations
            .iter()
            .map(|&(x, z)| {
                let (mx, my) = to_hexmap_coord(x, z);
                HeatPoint {
                    x: mx,
                    y: my,
                    intensity: 1.0,
                }
            })
            .collect();
    }

    let mut cells: HashMap<(i32, i32), u32> = HashMap::new();
    for &(x, z) in locations {
        let key = to_hexmap_coord(x, z);
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
