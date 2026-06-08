//! Road renderer — reads paved-tile and location JSON dumps and produces a
//! tile pyramid of colored road hexagons.
//!
//! Output: `{output_dir}/roads/tiles/{z}/{x}/{y}.webp`

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path as FsPath;
use std::sync::atomic::AtomicBool;
use tiny_skia::{Color, FillRule, Paint, PathBuilder, Pixmap, Transform};

use crate::tile_generator::{self, check_canceled};

/// Pixel canvas size = WORLD_TILES × RENDER_SCALE = 12800 × 3 = 38400. Must match other renderers.
const MAP_SIZE: u32 = 38400;

const COLOR_ENTRIES: &[(&str, &str)] = &[
    ("gilded", "#FFD700"),
    ("ruins", "#696969"),
    ("dark", "#3D2B1F"),
    ("dirt", "#8B4513"),
    ("gray", "#808080"),
    ("black", "#2C2C2C"),
    ("red", "#CD5C5C"),
    ("teal", "#008080"),
    ("blue", "#4169E1"),
    ("purple", "#9370DB"),
    ("green", "#228B22"),
    ("yellow", "#DAA520"),
    ("orange", "#FF8C00"),
    ("white", "#E8E8E8"),
    ("plank", "#A0522D"),
];

const COLOR_UNKNOWN: &str = "#FF00FF";
const COLOR_DEFAULT: &str = "#8B4513"; // dirt brown

#[derive(Debug, Deserialize)]
struct PavedTileRow {
    entity_id: u64,
    tile_type_id: i32,
}

#[derive(Debug, Deserialize)]
struct RoadLocationRow {
    entity_id: u64,
    x: f32,
    z: f32,
    dimension: i32,
}

#[derive(Debug, Deserialize)]
struct PavingTileDesc {
    id: i32,
    name: String,
}

/// Load road data from `input_dir` across all available region subdirectories
/// and render the road tile pyramid.
///
/// Reads:
///   `{input_dir}/global/paving_tile_desc.json`  (mandatory)
///   `{input_dir}/{region_prefix}{N}/paved_tile_state.json`
///   `{input_dir}/{region_prefix}{N}/road_locations.json`
///
/// Tiles are written directly into `tiles_dir` (`{z}/{x}/{y}.webp`).
pub fn render(
    input_dir: &FsPath,
    region_prefix: &str,
    tiles_dir: &FsPath,
    canceled: &AtomicBool,
) -> Result<()> {
    std::fs::create_dir_all(tiles_dir)
        .with_context(|| format!("Failed to create {}", tiles_dir.display()))?;

    let tile_colors = load_tile_color_map(input_dir)?;

    let mut all_locations: HashMap<u64, (f32, f32)> = HashMap::new();
    let mut all_tiles: Vec<PavedTileRow> = Vec::new();

    for region_id in 1..=25u32 {
        let tiles_path = input_dir.join(format!(
            "{}{}/paved_tile_state.json",
            region_prefix, region_id
        ));
        let locs_path = input_dir.join(format!(
            "{}{}/road_locations.json",
            region_prefix, region_id
        ));

        if !tiles_path.exists() && !locs_path.exists() {
            continue;
        }
        check_canceled(canceled)?;

        if let Ok(f) = File::open(&locs_path) {
            let locs: Vec<RoadLocationRow> = serde_json::from_reader(BufReader::new(f))
                .with_context(|| format!("Failed to parse {}", locs_path.display()))?;
            let surface: usize = locs.iter().filter(|l| l.dimension == 1).count();
            log::debug!(
                "[roads] region {}: {} surface locations",
                region_id,
                surface
            );
            for loc in locs {
                if loc.dimension == 1 {
                    all_locations.insert(loc.entity_id, (loc.x, loc.z));
                }
            }
        }

        if let Ok(f) = File::open(&tiles_path) {
            let tiles: Vec<PavedTileRow> = serde_json::from_reader(BufReader::new(f))
                .with_context(|| format!("Failed to parse {}", tiles_path.display()))?;
            log::debug!("[roads] region {}: {} paved tiles", region_id, tiles.len());
            all_tiles.extend(tiles);
        }
    }

    if all_tiles.is_empty() || all_locations.is_empty() {
        anyhow::bail!("[roads] no road data found in {}", input_dir.display());
    }

    let mut unknown_tile_types: HashMap<i32, u64> = HashMap::new();
    let mut points: Vec<(f32, f32, String)> = Vec::with_capacity(all_tiles.len());

    for row in &all_tiles {
        let Some(&(x, z)) = all_locations.get(&row.entity_id) else {
            continue;
        };
        let color = match tile_colors.get(&row.tile_type_id) {
            Some(c) => c.clone(),
            None => {
                *unknown_tile_types.entry(row.tile_type_id).or_insert(0) += 1;
                COLOR_UNKNOWN.to_string()
            }
        };
        points.push((x, z, color));
    }

    if !unknown_tile_types.is_empty() {
        let mut sorted: Vec<_> = unknown_tile_types.iter().collect();
        sorted.sort_by_key(|&(id, _)| id);
        for (id, count) in &sorted {
            log::warn!(
                "[roads] unknown tile type id {} — {} points (falling back to magenta)",
                id,
                count
            );
        }
    }
    log::info!("[roads] rendering {} road hexagons", points.len());

    let raw_data = render_hexagons(points, MAP_SIZE, MAP_SIZE)?;

    log::info!("[roads] generating tile pyramid → {}", tiles_dir.display());
    tile_generator::generate_tiles_from_raw(&raw_data, MAP_SIZE, MAP_SIZE, tiles_dir, canceled)?;

    log::info!("[roads] done");
    Ok(())
}

fn get_color_for_tile_name(name: &str) -> String {
    let lower = name.to_lowercase();
    for &(pattern, color) in COLOR_ENTRIES {
        if lower.contains(pattern) {
            return color.to_string();
        }
    }
    COLOR_DEFAULT.to_string()
}

fn load_tile_color_map(input_dir: &FsPath) -> Result<HashMap<i32, String>> {
    let desc_path = input_dir.join("global").join("paving_tile_desc.json");
    let f = File::open(&desc_path)
        .with_context(|| format!("Failed to open {}", desc_path.display()))?;
    let descs: Vec<PavingTileDesc> = serde_json::from_reader(BufReader::new(f))
        .with_context(|| format!("Failed to parse {}", desc_path.display()))?;
    log::debug!("[roads] loaded {} paving tile descriptions", descs.len());
    Ok(descs
        .into_iter()
        .map(|d| (d.id, get_color_for_tile_name(&d.name)))
        .collect())
}

fn parse_hex_color(hex: &str) -> (u8, u8, u8, u8) {
    let hex = hex.trim_start_matches('#');
    if hex.len() >= 6 {
        let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(255);
        let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(0);
        let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(255);
        (r, g, b, 255)
    } else {
        (255, 0, 255, 255) // fallback pink
    }
}

fn hexagon_path(cx: f32, cy: f32, apothem: f32) -> Option<tiny_skia::Path> {
    let side = (2.0 * apothem) / 3.0_f32.sqrt();
    let r = side;
    let angles: [f32; 6] = [90.0, 150.0, 210.0, 270.0, 330.0, 30.0];
    let vertices: Vec<(f32, f32)> = angles
        .iter()
        .map(|deg| {
            let rad = deg.to_radians();
            (cx + r * rad.cos(), cy + r * rad.sin())
        })
        .collect();

    let mut pb = PathBuilder::new();
    pb.move_to(vertices[0].0, vertices[0].1);
    for v in vertices.iter().skip(1) {
        pb.line_to(v.0, v.1);
    }
    pb.close();
    pb.finish()
}

fn render_hexagons(points: Vec<(f32, f32, String)>, width: u32, height: u32) -> Result<Vec<u8>> {
    let mut pixmap =
        Pixmap::new(width, height).context("Failed to create pixmap (out of memory?)")?;

    // Group by color for batch rendering.
    let mut by_color: HashMap<String, Vec<(f32, f32)>> = HashMap::new();
    for (x, z, color) in points {
        by_color.entry(color).or_default().push((x, z));
    }

    for (color_str, pts) in &by_color {
        let (r, g, b, a) = parse_hex_color(color_str);
        let color = Color::from_rgba8(r, g, b, a);
        let mut paint = Paint::<'_> {
            anti_alias: false,
            ..Default::default()
        };
        paint.set_color(color);

        for &(x, z) in pts {
            let px = x;
            let py = (height as f32) - 1.0 - z;
            if let Some(path) = hexagon_path(px + 0.5, py + 0.5, 0.5) {
                pixmap.fill_path(
                    &path,
                    &paint,
                    FillRule::Winding,
                    Transform::identity(),
                    None,
                );
            }
        }
    }

    Ok(pixmap.take())
}
