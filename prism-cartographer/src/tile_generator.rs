use anyhow::{Context, Result};
use image::imageops::FilterType;
use image::{ImageBuffer, Rgba, RgbaImage};
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};

const SQRT_3: f64 = 1.7320508075688772;
const CRS_APOTHEM: f64 = 2.0 / SQRT_3;
const RENDER_SIZE: f64 = 38400.0; // pixels at zoom 0
const TILE_PX: u32 = 256;
const MIN_ZOOM: i32 = -5;
const MAX_ZOOM: i32 = 0;

#[derive(Clone, Copy, Debug)]
pub enum TileScaling {
    Nearest,
    Lanczos3,
}

struct TileRange {
    x_min: i32,
    x_max: i32,
    y_min: i32,
    y_max: i32,
}

fn get_tile_range(crs_zoom: i32) -> TileRange {
    let scale = 2.0_f64.powi(crs_zoom);
    let px_max_x = RENDER_SIZE * scale;
    let px_min_y = -(RENDER_SIZE / CRS_APOTHEM) * scale;
    TileRange {
        x_min: 0,
        x_max: (px_max_x / TILE_PX as f64).ceil() as i32 - 1,
        y_min: (px_min_y / TILE_PX as f64).floor() as i32,
        y_max: -1,
    }
}

/// Generate a Leaflet-compatible WebP tile pyramid from a full-resolution
/// RGBA image buffer.
///
/// Checks `canceled` at the start of each zoom level and each tile column;
/// returns an error immediately if it is set.
///
/// Output: `{output_dir}/{zoom}/{x}/{y}.webp`
pub fn generate_tiles(
    img: &ImageBuffer<Rgba<u8>, Vec<u8>>,
    output_dir: &std::path::Path,
    scaling: TileScaling,
    canceled: &AtomicBool,
) -> Result<()> {
    let (img_w, img_h) = img.dimensions();
    let mut total = 0u32;

    for crs_zoom in MIN_ZOOM..=MAX_ZOOM {
        check_canceled(canceled)?;

        let scale = 2.0_f64.powi(crs_zoom);
        let range = get_tile_range(crs_zoom);

        let num_x = range.x_max - range.x_min + 1;
        let num_y = range.y_max - range.y_min + 1;

        let proj_w = (RENDER_SIZE * scale).round() as u32;
        let proj_h = ((RENDER_SIZE / CRS_APOTHEM) * scale).round() as u32;

        let pad_h = (num_y as u32) * TILE_PX;
        let top_pad = pad_h - proj_h;

        log::info!(
            "[tiles] zoom {}: {}×{} tiles, projected {}×{}",
            crs_zoom,
            num_x,
            num_y,
            proj_w,
            proj_h
        );

        let resized = match scaling {
            TileScaling::Nearest => nn_downscale(img, img_w, img_h, proj_w, proj_h),
            TileScaling::Lanczos3 => image::imageops::resize(img, proj_w, proj_h, FilterType::Lanczos3),
        };

        let mut count = 0u32;
        for tx in range.x_min..=range.x_max {
            check_canceled(canceled)?;

            let col_dir = output_dir.join(format!("{}/{}", crs_zoom, tx));
            fs::create_dir_all(&col_dir)
                .with_context(|| format!("Failed to create tile dir: {}", col_dir.display()))?;

            for ty in range.y_min..=range.y_max {
                let tile_px = ((tx - range.x_min) as u32) * TILE_PX;
                let tile_py = ((ty - range.y_min) as u32) * TILE_PX;

                let tile = RgbaImage::from_fn(TILE_PX, TILE_PX, |tpx, tpy| {
                    let src_x = tile_px + tpx;
                    let padded_y = tile_py + tpy;
                    if padded_y >= top_pad && src_x < proj_w {
                        let src_y = padded_y - top_pad;
                        if src_y < proj_h {
                            return *resized.get_pixel(src_x, src_y);
                        }
                    }
                    Rgba([0, 0, 0, 0])
                });

                let path = col_dir.join(format!("{}.webp", ty));
                encode_webp_lossless(&tile, &path)?;
                count += 1;
            }
        }

        total += count;
        log::info!("[tiles] zoom {} → {} tiles", crs_zoom, count);
    }

    log::info!("[tiles] total tiles generated: {}", total);
    Ok(())
}

/// Same as `generate_tiles` but accepts raw RGBA bytes + dimensions.
pub fn generate_tiles_from_raw(
    data: &[u8],
    width: u32,
    height: u32,
    output_dir: &std::path::Path,
    scaling: TileScaling,
    canceled: &AtomicBool,
) -> Result<()> {
    let owned: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::from_raw(width, height, data.to_vec())
        .context("Invalid image dimensions for raw data")?;
    generate_tiles(&owned, output_dir, scaling, canceled)
}

fn nn_downscale(
    img: &ImageBuffer<Rgba<u8>, Vec<u8>>,
    src_w: u32,
    src_h: u32,
    dst_w: u32,
    dst_h: u32,
) -> ImageBuffer<Rgba<u8>, Vec<u8>> {
    ImageBuffer::from_fn(dst_w, dst_h, |dx, dy| {
        let sx = (dx as u64 * src_w as u64 / dst_w as u64).min(src_w as u64 - 1) as u32;
        let sy = (dy as u64 * src_h as u64 / dst_h as u64).min(src_h as u64 - 1) as u32;
        *img.get_pixel(sx, sy)
    })
}

fn encode_webp_lossless(img: &RgbaImage, path: &std::path::Path) -> Result<()> {
    use image::ExtendedColorType;
    use image::codecs::webp::WebPEncoder;
    use std::io::BufWriter;

    let (w, h) = img.dimensions();
    let file =
        fs::File::create(path).with_context(|| format!("Failed to create {}", path.display()))?;
    WebPEncoder::new_lossless(BufWriter::new(file))
        .encode(img.as_raw(), w, h, ExtendedColorType::Rgba8)
        .with_context(|| format!("Failed to encode {}", path.display()))?;
    Ok(())
}

pub(crate) fn check_canceled(canceled: &AtomicBool) -> Result<()> {
    if canceled.load(Ordering::Relaxed) {
        anyhow::bail!("canceled")
    }
    Ok(())
}
