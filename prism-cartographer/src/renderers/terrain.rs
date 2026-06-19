//! Terrain map renderer — reads JSON dumps and produces a tile pyramid.
//!
//! Output: `{output_dir}/maps/terrain/tiles/{z}/{x}/{y}.webp`

use anyhow::{Context, Result};
use image::{ImageBuffer, Rgba};
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::tile_generator::{self, check_canceled};

pub const BIOME_GRID: u32 = 32;
pub const BIOME_TILES_PER_CHUNK: u32 = BIOME_GRID * BIOME_GRID;
/// Number of chunks along each axis per region side.
pub const CHUNKS_PER_REGION_SIDE: u32 = 80;
/// Maximum number of regions in the world (5×5 grid).
pub const REGION_COUNT: u32 = 25;
/// Total chunks along each world axis (5 regions × 80 chunks).
pub const WORLD_CHUNKS: u32 = REGION_COUNT / 5 * CHUNKS_PER_REGION_SIDE * 5 / 5; // = 5*80 = 400
pub const WORLD_TILES: u32 = WORLD_CHUNKS * BIOME_GRID; // 12800 biome tiles per axis

/// Pixels per biome tile in the full-resolution render.
pub const RENDER_SCALE: u32 = 3;
/// Full-resolution image dimension (must equal game RENDER_SIZE = 38400).
pub const RENDER_SIZE: u32 = WORLD_TILES * RENDER_SCALE; // 38400

const RELIEF_STRENGTH: f32 = 0.5;
const RELIEF_MAX_DIFF: f32 = 15.0;
const NO_DATA_ELEVATION: i16 = i16::MIN;

const WATER_SHALLOW_COLOR: [u8; 3] = [55, 75, 100];
const WATER_DEEP_COLOR: [u8; 3] = [30, 40, 65];
const WATER_MAX_DEPTH: f32 = 25.0;

#[derive(Debug, Deserialize)]
pub struct BiomeDesc {
    pub biome_type: u8,
    pub name: String,
}

fn deserialize_string_as_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.into_bytes())
}

#[derive(Debug, Deserialize)]
pub struct TerrainChunkState {
    pub chunk_x: i32,
    pub chunk_z: i32,
    pub dimension: u32,
    pub biomes: Vec<u32>,
    pub biome_density: Vec<u32>,
    pub elevations: Vec<i16>,
    pub water_levels: Vec<i16>,
    #[serde(deserialize_with = "deserialize_string_as_bytes")]
    pub water_body_types: Vec<u8>,
}

/// Load all region JSON data from `input_dir` and render the terrain tile pyramid.
///
/// Reads:
///   `{input_dir}/global/biome_desc.json`  (mandatory)
///   `{input_dir}/{region_prefix}{N}/terrain_chunk_state.json`
///
/// Tiles are written directly into `tiles_dir` (`{z}/{x}/{y}.webp`).
pub fn render(
    input_dir: &Path,
    region_prefix: &str,
    tiles_dir: &Path,
    canceled: &AtomicBool,
) -> Result<()> {
    std::fs::create_dir_all(tiles_dir)
        .with_context(|| format!("Failed to create {}", tiles_dir.display()))?;

    log::info!(
        "[terrain] Rendering {}×{} px world map ({} biome tiles/axis, scale ×{})",
        RENDER_SIZE,
        RENDER_SIZE,
        WORLD_TILES,
        RENDER_SCALE
    );

    let biome_path = input_dir.join("global").join("biome_desc.json");
    let biome_descs: Vec<BiomeDesc> = {
        let f = File::open(&biome_path)
            .with_context(|| format!("Failed to open {}", biome_path.display()))?;
        serde_json::from_reader(BufReader::new(f))
            .with_context(|| format!("Failed to parse {}", biome_path.display()))?
    };
    let known_biomes: HashSet<u8> = biome_descs.iter().map(|b| b.biome_type).collect();
    let biome_names: HashMap<u8, &str> = biome_descs
        .iter()
        .map(|b| (b.biome_type, b.name.as_str()))
        .collect();
    log::debug!("[terrain] loaded {} biome types", biome_descs.len());
    for desc in &biome_descs {
        let color = biome_color(desc.biome_type);
        log::debug!(
            "[terrain]   {:2} - {:<25} RGB({:3}, {:3}, {:3})",
            desc.biome_type,
            desc.name,
            color[0],
            color[1],
            color[2]
        );
    }
    let _ = biome_names; // used only for the debug log above

    let mut regions: Vec<(u32, Vec<TerrainChunkState>)> = Vec::new();
    for region_id in 1..=REGION_COUNT {
        let terrain_path = input_dir.join(format!(
            "{}{}/terrain_chunk_state.json",
            region_prefix, region_id
        ));

        let chunks: Vec<TerrainChunkState> = match File::open(&terrain_path) {
            Ok(f) => serde_json::from_reader(BufReader::new(f))
                .with_context(|| format!("Failed to parse {}", terrain_path.display()))?,
            Err(_) => {
                log::debug!("[terrain] region {} not found, skipping", region_id);
                continue;
            }
        };

        log::debug!(
            "[terrain] loaded region {} ({} chunks)",
            region_id,
            chunks.len()
        );
        regions.push((region_id, chunks));
    }

    if regions.is_empty() {
        anyhow::bail!("[terrain] no region data found in {}", input_dir.display());
    }

    let mut img: ImageBuffer<Rgba<u8>, Vec<u8>> =
        ImageBuffer::from_fn(RENDER_SIZE, RENDER_SIZE, |_, _| Rgba([0, 0, 0, 0]));

    let mut elevations: Vec<i16> = vec![NO_DATA_ELEVATION; (WORLD_TILES * WORLD_TILES) as usize];
    let mut water_types: Vec<u8> = vec![0u8; (WORLD_TILES * WORLD_TILES) as usize];

    let mut total_chunks = 0u64;
    let mut unknown_biomes: HashMap<u8, u64> = HashMap::new();

    for (region_id, chunks) in &regions {
        check_canceled(canceled)?;

        let surface_chunks: Vec<&TerrainChunkState> =
            chunks.iter().filter(|c| c.dimension == 1).collect();

        log::debug!(
            "[terrain] region {}: {} surface chunks",
            region_id,
            surface_chunks.len()
        );

        for chunk in &surface_chunks {
            if chunk.biomes.len() != BIOME_TILES_PER_CHUNK as usize {
                log::warn!(
                    "[terrain] chunk ({},{}) has {} biomes (expected {}), skipping",
                    chunk.chunk_x,
                    chunk.chunk_z,
                    chunk.biomes.len(),
                    BIOME_TILES_PER_CHUNK
                );
                continue;
            }
            if chunk.chunk_x < 0 || chunk.chunk_z < 0 {
                log::warn!(
                    "[terrain] skipping chunk with negative coords ({}, {})",
                    chunk.chunk_x,
                    chunk.chunk_z
                );
                continue;
            }

            for tile_idx in 0..BIOME_TILES_PER_CHUNK {
                let local_x = tile_idx % BIOME_GRID;
                let local_z = tile_idx / BIOME_GRID;

                let world_x = (chunk.chunk_x as u32) * BIOME_GRID + local_x;
                let world_z = (chunk.chunk_z as u32) * BIOME_GRID + local_z;

                let idx = tile_idx as usize;
                let raw_biome = chunk.biomes[idx];
                let base_biome = (raw_biome & 0xFF) as u8;

                if base_biome != 0 && !known_biomes.contains(&base_biome) {
                    *unknown_biomes.entry(base_biome).or_insert(0) += 1;
                }

                let density = chunk.biome_density.get(idx).copied().unwrap_or(0);
                let mut color = blended_biome_color(raw_biome, density);

                let water_type = chunk.water_body_types.get(idx).copied().unwrap_or(0);
                let elevation = chunk.elevations.get(idx).copied().unwrap_or(0);
                let water_level = chunk.water_levels.get(idx).copied().unwrap_or(0);

                let depth = water_level - elevation;
                let effective_water_type;

                if water_type > 0 {
                    if depth > 0 {
                        effective_water_type = water_type;
                        color = depth_water_color(depth);
                    } else {
                        effective_water_type = 0;
                    }
                } else if depth > 0 {
                    effective_water_type = 1;
                    color = depth_water_color(depth);
                } else {
                    effective_water_type = 0;
                }

                let bx = world_x;
                let by = WORLD_TILES - 1 - world_z;

                if bx < WORLD_TILES && by < WORLD_TILES {
                    let rgba = Rgba([color[0], color[1], color[2], 255]);
                    for sy in 0..RENDER_SCALE {
                        for sx in 0..RENDER_SCALE {
                            img.put_pixel(bx * RENDER_SCALE + sx, by * RENDER_SCALE + sy, rgba);
                        }
                    }
                }

                let ti = (by * WORLD_TILES + bx) as usize;
                if ti < elevations.len() {
                    elevations[ti] = elevation;
                    water_types[ti] = effective_water_type;
                }
            }
        }

        total_chunks += surface_chunks.len() as u64;
    }

    if !unknown_biomes.is_empty() {
        let mut sorted: Vec<_> = unknown_biomes.iter().collect();
        sorted.sort_by_key(|&(id, _)| id);
        for (id, count) in &sorted {
            log::warn!(
                "[terrain] unknown biome type {} — {} tiles (falling back to magenta)",
                id,
                count
            );
        }
    }
    log::info!("[terrain] total surface chunks rendered: {}", total_chunks);

    log::debug!("[terrain] applying elevation relief...");
    apply_relief(&mut img, &elevations, &water_types, canceled)?;
    log::debug!("[terrain] elevation relief done");

    drop(elevations);
    drop(water_types);

    log::info!(
        "[terrain] generating tile pyramid → {}",
        tiles_dir.display()
    );
    tile_generator::generate_tiles(
        &img,
        tiles_dir,
        tile_generator::TileScaling::Nearest,
        canceled,
    )?;

    log::info!("[terrain] done");
    Ok(())
}

fn apply_relief(
    img: &mut ImageBuffer<Rgba<u8>, Vec<u8>>,
    elevations: &[i16],
    water_types: &[u8],
    canceled: &AtomicBool,
) -> Result<()> {
    for by in 0..WORLD_TILES {
        check_canceled(canceled)?;
        for bx in 0..WORLD_TILES {
            let ti = (by * WORLD_TILES + bx) as usize;
            let elev = elevations[ti];
            if elev == NO_DATA_ELEVATION || water_types[ti] > 0 {
                continue;
            }

            let mut neighbor_sum = 0.0f32;
            let mut neighbor_count = 0u32;
            for (nx, ny) in hex_neighbors(bx as i32, by as i32) {
                if nx < 0 || ny < 0 || nx >= WORLD_TILES as i32 || ny >= WORLD_TILES as i32 {
                    continue;
                }
                let ni = (ny as u32 * WORLD_TILES + nx as u32) as usize;
                let n_elev = elevations[ni];
                if n_elev == NO_DATA_ELEVATION {
                    continue;
                }
                neighbor_sum += n_elev as f32;
                neighbor_count += 1;
            }
            if neighbor_count == 0 {
                continue;
            }

            let avg = neighbor_sum / neighbor_count as f32;
            let diff = elev as f32 - avg;
            let t = (diff / RELIEF_MAX_DIFF).clamp(-1.0, 1.0);
            let brightness = 1.0 + t * RELIEF_STRENGTH;

            for sy in 0..RENDER_SCALE {
                for sx in 0..RENDER_SCALE {
                    let rpx = bx * RENDER_SCALE + sx;
                    let rpy = by * RENDER_SCALE + sy;
                    let p = img.get_pixel(rpx, rpy);
                    let r = (p[0] as f32 * brightness).clamp(0.0, 255.0) as u8;
                    let g = (p[1] as f32 * brightness).clamp(0.0, 255.0) as u8;
                    let b = (p[2] as f32 * brightness).clamp(0.0, 255.0) as u8;
                    img.put_pixel(rpx, rpy, Rgba([r, g, b, 255]));
                }
            }
        }
    }
    Ok(())
}

fn hex_neighbors(tx: i32, tz: i32) -> [(i32, i32); 6] {
    if tz & 1 == 0 {
        [
            (tx - 1, tz - 1),
            (tx, tz - 1),
            (tx - 1, tz),
            (tx + 1, tz),
            (tx - 1, tz + 1),
            (tx, tz + 1),
        ]
    } else {
        [
            (tx, tz - 1),
            (tx + 1, tz - 1),
            (tx - 1, tz),
            (tx + 1, tz),
            (tx, tz + 1),
            (tx + 1, tz + 1),
        ]
    }
}

fn biome_color(biome_type: u8) -> [u8; 3] {
    match biome_type {
        0 => [255, 0, 255],
        1 => [65, 74, 52],
        2 => [35, 48, 38],
        3 => [140, 135, 138],
        4 => [158, 143, 87],
        5 => [85, 70, 70],
        6 => [80, 80, 95],
        7 => [100, 95, 78],
        8 => [42, 48, 42],
        9 => [76, 74, 87],
        10 => [42, 49, 69],
        11 => [101, 109, 83],
        12 => [50, 48, 52],
        13 => [32, 55, 40],
        14 => [70, 63, 77],
        15 => [219, 208, 125],
        16 => [14, 77, 2],
        17 => [94, 59, 22],
        18 => [67, 213, 238],
        _ => [255, 0, 255],
    }
}

fn depth_water_color(depth: i16) -> [u8; 3] {
    let t = (depth as f32 / WATER_MAX_DEPTH).clamp(0.0, 1.0);
    [
        (WATER_SHALLOW_COLOR[0] as f32 * (1.0 - t) + WATER_DEEP_COLOR[0] as f32 * t) as u8,
        (WATER_SHALLOW_COLOR[1] as f32 * (1.0 - t) + WATER_DEEP_COLOR[1] as f32 * t) as u8,
        (WATER_SHALLOW_COLOR[2] as f32 * (1.0 - t) + WATER_DEEP_COLOR[2] as f32 * t) as u8,
    ]
}

fn blended_biome_color(biomes: u32, biome_density: u32) -> [u8; 3] {
    let mut r = 0.0f32;
    let mut g = 0.0f32;
    let mut b = 0.0f32;
    let mut total_weight = 0.0f32;

    for i in 0..4u32 {
        let biome_index = ((biomes >> (i * 8)) & 0xFF) as u8;
        let density = ((biome_density >> (i * 8)) & 0xFF) as f32 / 128.0;
        if density <= 0.0 {
            continue;
        }
        let c = biome_color(biome_index);
        r += c[0] as f32 * density;
        g += c[1] as f32 * density;
        b += c[2] as f32 * density;
        total_weight += density;
    }

    if total_weight > 0.0 {
        [
            (r / total_weight).clamp(0.0, 255.0) as u8,
            (g / total_weight).clamp(0.0, 255.0) as u8,
            (b / total_weight).clamp(0.0, 255.0) as u8,
        ]
    } else {
        biome_color((biomes & 0xFF) as u8)
    }
}
