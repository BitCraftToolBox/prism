//! Game-style map renderer — downloads a `.gwm` file from the Bitcraft map
//! server and produces a tile pyramid matching the terrain renderer's CRS.
//!
//! Output: `{output_dir}/maps/game/tiles/{z}/{x}/{y}.webp`

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use image::{ImageBuffer, Rgb};
use metrics::gauge;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::tile_generator::{self, check_canceled};

const MAP_NAME: &str = "TerrainMap";

/// Width/height of the tile grid (5 regions × 80 chunks × 10 px/chunk).
const MAP_SIZE: u32 = 5 * 80 * 10; // 4000

/// Hex-grid aspect-ratio correction factor.
const HEX_RATIO: f64 = 1.1547005;

/// Bytes per tile record in the decompressed `.gwm` stream.
const BYTES_PER_TILE: usize = 8;
/// Header bytes to skip at the start of the decompressed stream.
const HEADER_BYTES: usize = 8;

/// Full-resolution pixel size – must match terrain renderer so tiles align.
const RENDER_SIZE: u32 = 38400;

/// Render the game-style map by downloading `TerrainMap.gwm` from the Bitcraft
/// map server and producing a WebP tile pyramid.
///
/// Tiles are written directly into `tiles_dir` (`{z}/{x}/{y}.webp`).
pub fn render(game_map_url: &str, tiles_dir: &Path, canceled: &AtomicBool) -> Result<()> {
    std::fs::create_dir_all(tiles_dir)
        .with_context(|| format!("Failed to create {}", tiles_dir.display()))?;

    let data = download_gwm(game_map_url, MAP_NAME)?;
    check_canceled(canceled)?;

    let width = MAP_SIZE;
    let height = MAP_SIZE;
    let available = (data.len().saturating_sub(HEADER_BYTES)) / BYTES_PER_TILE;
    log::info!(
        "[game] parsing {}×{} tiles ({} available in stream)",
        width,
        height,
        available
    );

    let mut full_img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::new(width, height);

    let mut offset = HEADER_BYTES;
    let mut x = 0u32;
    let mut y = 0u32;
    let mut tile_count = 0u64;

    while data.len() >= offset + BYTES_PER_TILE && y < height {
        let tile = &data[offset..offset + BYTES_PER_TILE];
        // byte layout: [?, ?, ?, R, G, B, ?, ?] → Python: col = (tile[3], tile[2], tile[1])
        let color = Rgb([tile[3], tile[2], tile[1]]);
        full_img.put_pixel(x, height - 1 - y, color);

        offset += BYTES_PER_TILE;
        tile_count += 1;
        x += 1;
        if x == width {
            y += 1;
            x = 0;
        }
    }

    log::info!("[game] parsed {} tiles", tile_count);

    // Hex-aspect-ratio correction: stretch vertically by HEX_RATIO.
    let scaled = stretch_vertical(&full_img, HEX_RATIO);

    drop(full_img);

    log::info!("[game] generating tile pyramid → {}", tiles_dir.display());
    tile_generator::generate_tiles_nearest(
        &NnUpscaled(&scaled),
        tiles_dir,
        canceled,
        "game",
    )?;

    log::info!("[game] done");
    Ok(())
}

fn download_gwm(url_root: &str, name: &str) -> Result<Vec<u8>> {
    let url = format!("{}/{}.gwm", url_root.trim_end_matches('/'), name);
    log::info!("[game] downloading {}", url);
    let dl_start = std::time::Instant::now();
    let compressed = reqwest::blocking::get(&url)
        .with_context(|| format!("Failed to fetch {}", url))?
        .bytes()
        .with_context(|| format!("Failed to read response from {}", url))?;

    gauge!("cartographer_download_last_duration_seconds").set(dl_start.elapsed().as_secs_f64());
    gauge!("cartographer_download_bytes").set(compressed.len() as f64);
    log::info!(
        "[game] downloaded {} bytes (compressed), decompressing...",
        compressed.len()
    );
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut data = Vec::new();
    decoder
        .read_to_end(&mut data)
        .context("Failed to decompress .gwm gzip data")?;
    log::info!("[game] decompressed to {} bytes", data.len());
    Ok(data)
}

/// Stretch the image content vertically by `ratio` using nearest-neighbor,
/// keeping the original dimensions (top content is cropped).
fn stretch_vertical(
    img: &ImageBuffer<Rgb<u8>, Vec<u8>>,
    ratio: f64,
) -> ImageBuffer<Rgb<u8>, Vec<u8>> {
    let (src_w, src_h) = img.dimensions();
    let offset = src_h as f64 - src_h as f64 / ratio;
    ImageBuffer::from_fn(src_w, src_h, |dx, dy| {
        let sy = (offset + dy as f64 / ratio).min(src_h as f64 - 1.0) as u32;
        *img.get_pixel(dx, sy)
    })
}

/// Presents the parsed RGB map as an opaque `RENDER_SIZE²` RGBA raster via
/// nearest-neighbor upscaling, computed per pixel.
struct NnUpscaled<'a>(&'a ImageBuffer<Rgb<u8>, Vec<u8>>);

impl tile_generator::PixelSource for NnUpscaled<'_> {
    fn dimensions(&self) -> (u32, u32) {
        (RENDER_SIZE, RENDER_SIZE)
    }

    #[inline]
    fn pixel(&self, x: u32, y: u32) -> [u8; 4] {
        let (src_w, src_h) = self.0.dimensions();
        let sx = (x as f64 * src_w as f64 / RENDER_SIZE as f64) as u32;
        let sy = (y as f64 * src_h as f64 / RENDER_SIZE as f64) as u32;
        let p = self.0.get_pixel(sx.min(src_w - 1), sy.min(src_h - 1));
        [p[0], p[1], p[2], 255]
    }
}
