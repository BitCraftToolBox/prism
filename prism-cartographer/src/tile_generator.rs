//! Leaflet-compatible WebP tile pyramid generation.
//!
//! Memory is bounded independently of zoom level: each zoom is produced one
//! tile row (256 output px) at a time into a small band buffer, and tiles are
//! sliced out of that band. The full-resolution projected image for a zoom
//! level is never materialized.
//!
//! - Nearest-neighbor ([`generate_tiles_nearest`]) samples a [`PixelSource`]
//!   directly per output pixel, so the source can be virtual (e.g. a small
//!   raster upscaled on the fly) and never has to exist at 38400² in RAM.
//! - Lanczos3 ([`generate_tiles_lanczos3`]) resizes each band in sub-chunks.
//!   Every sub-chunk copies only the source rectangle it needs plus a halo of
//!   the filter's support radius, so results match a whole-image resize
//!   without seams, while scratch memory stays around
//!   `LANCZOS_CHUNK_SRC_PX² × 4` bytes.

use anyhow::{Context, Result};
use fast_image_resize as fir;
use metrics::gauge;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const SQRT_3: f64 = 1.7320508075688772;
const CRS_APOTHEM: f64 = 2.0 / SQRT_3;
const RENDER_SIZE: f64 = 38400.0; // pixels at zoom 0
const TILE_PX: u32 = 256;
const MIN_ZOOM: i32 = -5;
const MAX_ZOOM: i32 = 0;

/// Target side length (in source pixels, halo included) of one Lanczos3
/// sub-chunk. 4096² × 4 B ≈ 64 MiB of premultiplied scratch per chunk.
const LANCZOS_CHUNK_SRC_PX: u32 = 4096;
/// Lanczos3 filter support (lobes), as used by `fast_image_resize`.
const LANCZOS3_SUPPORT: f64 = 3.0;

const BYTES_PER_PX: usize = 4;

/// A full-resolution RGBA image sampled by the nearest-neighbor pyramid.
///
/// Implementations may be backed by a real raster or computed on the fly
/// from a smaller one; only `pixel` is ever called, once per output pixel.
pub trait PixelSource {
    fn dimensions(&self) -> (u32, u32);
    fn pixel(&self, x: u32, y: u32) -> [u8; 4];
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

/// Generate the tile pyramid with nearest-neighbor sampling.
///
/// Output is byte-identical to downscaling the whole source to each zoom's
/// projected size and slicing it, but peak memory is one tile row.
pub fn generate_tiles_nearest<S: PixelSource>(
    src: &S,
    output_dir: &std::path::Path,
    canceled: &AtomicBool,
    task: &str,
) -> Result<()> {
    let (src_w, src_h) = src.dimensions();
    anyhow::ensure!(src_w > 0 && src_h > 0, "empty source image");

    let mut sx_table: Vec<u32> = Vec::new();
    let mut table_w = 0u32;

    generate_pyramid(output_dir, canceled, task, |proj_w, proj_h, p0, p1, band| {
        if table_w != proj_w {
            sx_table = (0..proj_w)
                .map(|dx| nn_index(dx, src_w, proj_w))
                .collect();
            table_w = proj_w;
        }
        let row_bytes = proj_w as usize * BYTES_PER_PX;
        for (py, row) in (p0..p1).zip(band.chunks_exact_mut(row_bytes)) {
            let sy = nn_index(py, src_h, proj_h);
            for (px, &sx) in row.chunks_exact_mut(BYTES_PER_PX).zip(&sx_table) {
                px.copy_from_slice(&src.pixel(sx, sy));
            }
        }
        Ok(())
    })
}

/// Generate the tile pyramid with Lanczos3 (premultiplied-alpha) resampling.
///
/// `data` is a tightly packed RGBA8 raster of `width × height`.
pub fn generate_tiles_lanczos3(
    data: &[u8],
    width: u32,
    height: u32,
    output_dir: &std::path::Path,
    canceled: &AtomicBool,
    task: &str,
) -> Result<()> {
    anyhow::ensure!(width > 0 && height > 0, "empty source image");
    anyhow::ensure!(
        data.len() >= width as usize * height as usize * BYTES_PER_PX,
        "raw RGBA buffer too small for {}×{}",
        width,
        height
    );

    let mut chunker = LanczosChunker {
        data,
        src_w: width,
        src_h: height,
        chunk_src_px: LANCZOS_CHUNK_SRC_PX,
        resizer: fir::Resizer::new(),
        mul_div: fir::MulDiv::default(),
        scratch: Vec::new(),
        dst: Vec::new(),
    };

    generate_pyramid(output_dir, canceled, task, |proj_w, proj_h, p0, p1, band| {
        chunker.fill_band(proj_w, proj_h, p0, p1, band)
    })
}

/// Shared zoom/tile-row loop.
///
/// For each zoom, walks tile rows top to bottom. `fill_band(proj_w, proj_h,
/// p0, p1, band)` must write projected rows `p0..p1` (full projected width)
/// into `band`; the tiles covering that row are then cut out and encoded.
fn generate_pyramid(
    output_dir: &std::path::Path,
    canceled: &AtomicBool,
    task: &str,
    mut fill_band: impl FnMut(u32, u32, u32, u32, &mut [u8]) -> Result<()>,
) -> Result<()> {
    let mut total = 0u32;
    let mut band: Vec<u8> = Vec::new();
    let mut tile = vec![0u8; (TILE_PX * TILE_PX) as usize * BYTES_PER_PX];
    let tile_row_bytes = TILE_PX as usize * BYTES_PER_PX;

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

        for tx in range.x_min..=range.x_max {
            let col_dir = output_dir.join(format!("{}/{}", crs_zoom, tx));
            fs::create_dir_all(&col_dir)
                .with_context(|| format!("Failed to create tile dir: {}", col_dir.display()))?;
        }

        let proj_row_bytes = proj_w as usize * BYTES_PER_PX;
        let mut resize_time = Duration::ZERO;
        let mut encode_time = Duration::ZERO;
        let mut count = 0u32;
        let mut zoom_bytes: u64 = 0;

        for ty in range.y_min..=range.y_max {
            check_canceled(canceled)?;

            // Padded rows covered by this tile row, and their projected range.
            let pad_y0 = ((ty - range.y_min) as u32) * TILE_PX;
            let pad_y1 = pad_y0 + TILE_PX;
            let p0 = pad_y0.max(top_pad) - top_pad;
            let p1 = (pad_y1.min(top_pad + proj_h)).saturating_sub(top_pad).max(p0);

            let resize_start = Instant::now();
            band.clear();
            band.resize((p1 - p0) as usize * proj_row_bytes, 0);
            if p1 > p0 {
                fill_band(proj_w, proj_h, p0, p1, &mut band)?;
            }
            resize_time += resize_start.elapsed();

            let encode_start = Instant::now();
            for tx in range.x_min..=range.x_max {
                let tile_x0 = ((tx - range.x_min) as u32) * TILE_PX;
                let copy_px = proj_w.saturating_sub(tile_x0).min(TILE_PX) as usize;

                tile.fill(0);
                if copy_px > 0 {
                    for p in p0..p1 {
                        let tpy = (p + top_pad - pad_y0) as usize;
                        let src_off =
                            (p - p0) as usize * proj_row_bytes + tile_x0 as usize * BYTES_PER_PX;
                        let dst_off = tpy * tile_row_bytes;
                        let n = copy_px * BYTES_PER_PX;
                        tile[dst_off..dst_off + n].copy_from_slice(&band[src_off..src_off + n]);
                    }
                }

                let path = output_dir.join(format!("{}/{}/{}.webp", crs_zoom, tx, ty));
                encode_webp_lossless(&tile, TILE_PX, TILE_PX, &path)?;
                zoom_bytes += path.metadata().map(|m| m.len()).unwrap_or(0);
                count += 1;
            }
            encode_time += encode_start.elapsed();
        }

        let zoom_label = crs_zoom.to_string();
        gauge!("cartographer_resize_last_duration_seconds", "task" => task.to_string(), "zoom" => zoom_label.clone())
            .set(resize_time.as_secs_f64());
        gauge!("cartographer_encode_last_duration_seconds", "task" => task.to_string(), "zoom" => zoom_label.clone())
            .set(encode_time.as_secs_f64());
        gauge!("cartographer_tile_bytes_total", "task" => task.to_string(), "zoom" => zoom_label)
            .set(zoom_bytes as f64);

        total += count;
        log::info!("[tiles] zoom {} → {} tiles", crs_zoom, count);
    }

    log::info!("[tiles] total tiles generated: {}", total);
    Ok(())
}

/// Nearest-neighbor source index for destination index `d` (same mapping as
/// a whole-image NN downscale from `src_len` to `dst_len`).
#[inline]
fn nn_index(d: u32, src_len: u32, dst_len: u32) -> u32 {
    (d as u64 * src_len as u64 / dst_len as u64).min(src_len as u64 - 1) as u32
}

/// Lanczos3 resize of arbitrary projected-space rectangles from a large
/// source raster, using only a bounded source window per call.
struct LanczosChunker<'a> {
    data: &'a [u8],
    src_w: u32,
    src_h: u32,
    /// Target source-window side length per chunk (see LANCZOS_CHUNK_SRC_PX).
    chunk_src_px: u32,
    resizer: fir::Resizer,
    mul_div: fir::MulDiv,
    /// Premultiplied copy of the source window (reused between chunks).
    scratch: Vec<u8>,
    /// Resized output of one chunk (reused between chunks).
    dst: Vec<u8>,
}

impl LanczosChunker<'_> {
    /// Fill projected rows `p0..p1` (full width) of a `proj_w × proj_h`
    /// downscale into `band`.
    fn fill_band(&mut self, proj_w: u32, proj_h: u32, p0: u32, p1: u32, band: &mut [u8]) -> Result<()> {
        let scale_x = self.src_w as f64 / proj_w as f64;
        let scale_y = self.src_h as f64 / proj_h as f64;
        let halo_x = filter_halo(scale_x);
        let halo_y = filter_halo(scale_y);

        // Largest destination chunk whose source window (+ halo) stays within
        // `chunk_src_px` on each side.
        let chunk_w = chunk_dst_len(self.chunk_src_px, scale_x, halo_x).min(proj_w);
        let chunk_h = chunk_dst_len(self.chunk_src_px, scale_y, halo_y).min(p1 - p0);

        let band_row_bytes = proj_w as usize * BYTES_PER_PX;
        let mut y0 = p0;
        while y0 < p1 {
            let y1 = (y0 + chunk_h).min(p1);
            let mut x0 = 0;
            while x0 < proj_w {
                let x1 = (x0 + chunk_w).min(proj_w);
                self.resize_rect(x0, x1, y0, y1, scale_x, scale_y, halo_x, halo_y)?;

                let n = (x1 - x0) as usize * BYTES_PER_PX;
                for (i, row) in self.dst.chunks_exact(n).enumerate() {
                    let off = (y0 - p0) as usize * band_row_bytes
                        + i * band_row_bytes
                        + x0 as usize * BYTES_PER_PX;
                    band[off..off + n].copy_from_slice(row);
                }
                x0 = x1;
            }
            y0 = y1;
        }
        Ok(())
    }

    /// Resize the projected rectangle `[x0,x1) × [y0,y1)` into `self.dst`.
    #[allow(clippy::too_many_arguments)]
    fn resize_rect(
        &mut self,
        x0: u32,
        x1: u32,
        y0: u32,
        y1: u32,
        scale_x: f64,
        scale_y: f64,
        halo_x: f64,
        halo_y: f64,
    ) -> Result<()> {
        // Exact source-space crop for this destination rectangle.
        let left = x0 as f64 * scale_x;
        let right = x1 as f64 * scale_x;
        let top = y0 as f64 * scale_y;
        let bottom = y1 as f64 * scale_y;

        // Source window: crop plus the filter's reach, clamped to the image.
        // At real image edges the clamp mirrors what a whole-image resize
        // does; elsewhere the halo covers every tap so there are no seams.
        let wx0 = (left - halo_x).floor().max(0.0) as u32;
        let wx1 = ((right + halo_x).ceil() as u32).min(self.src_w);
        let wy0 = (top - halo_y).floor().max(0.0) as u32;
        let wy1 = ((bottom + halo_y).ceil() as u32).min(self.src_h);
        let (ww, wh) = (wx1 - wx0, wy1 - wy0);

        let src_row_bytes = self.src_w as usize * BYTES_PER_PX;
        let win_row_bytes = ww as usize * BYTES_PER_PX;
        self.scratch.clear();
        self.scratch.reserve(win_row_bytes * wh as usize);
        for y in wy0..wy1 {
            let off = y as usize * src_row_bytes + wx0 as usize * BYTES_PER_PX;
            self.scratch
                .extend_from_slice(&self.data[off..off + win_row_bytes]);
        }

        let mut win = fir::images::Image::from_slice_u8(
            ww,
            wh,
            &mut self.scratch,
            fir::PixelType::U8x4,
        )
        .context("Invalid Lanczos3 source window")?;
        self.mul_div
            .multiply_alpha_inplace(&mut win)
            .context("Failed to premultiply alpha")?;

        let (dw, dh) = (x1 - x0, y1 - y0);
        self.dst.clear();
        self.dst.resize(dw as usize * dh as usize * BYTES_PER_PX, 0);
        let mut dst = fir::images::Image::from_slice_u8(dw, dh, &mut self.dst, fir::PixelType::U8x4)
            .context("Invalid Lanczos3 destination chunk")?;

        let options = fir::ResizeOptions::new()
            .resize_alg(fir::ResizeAlg::Convolution(fir::FilterType::Lanczos3))
            .crop(left - wx0 as f64, top - wy0 as f64, right - left, bottom - top)
            .use_alpha(false);
        self.resizer
            .resize(&win, &mut dst, Some(&options))
            .context("Lanczos3 resize failed")?;

        self.mul_div
            .divide_alpha_inplace(&mut dst)
            .context("Failed to un-premultiply alpha")?;
        Ok(())
    }
}

/// Source-pixel reach of the Lanczos3 kernel at a given downscale factor,
/// plus a pixel of slack for floor/ceil rounding.
fn filter_halo(scale: f64) -> f64 {
    (LANCZOS3_SUPPORT * scale.max(1.0)).ceil() + 1.0
}

/// Destination length whose source span plus halo on both sides fits in
/// `chunk_src_px`. Always at least 1.
fn chunk_dst_len(chunk_src_px: u32, scale: f64, halo: f64) -> u32 {
    let usable = chunk_src_px as f64 - 2.0 * halo;
    ((usable / scale).floor() as u32).max(1)
}

fn encode_webp_lossless(data: &[u8], w: u32, h: u32, path: &std::path::Path) -> Result<()> {
    use image::ExtendedColorType;
    use image::codecs::webp::WebPEncoder;
    use std::io::BufWriter;

    let file =
        fs::File::create(path).with_context(|| format!("Failed to create {}", path.display()))?;
    WebPEncoder::new_lossless(BufWriter::new(file))
        .encode(data, w, h, ExtendedColorType::Rgba8)
        .with_context(|| format!("Failed to encode {}", path.display()))?;
    Ok(())
}

pub(crate) fn check_canceled(canceled: &AtomicBool) -> Result<()> {
    if canceled.load(Ordering::Relaxed) {
        anyhow::bail!("canceled")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic sparse "road-like" RGBA image: mostly transparent with
    /// opaque colored runs, plus some gradients to exercise the filter.
    fn test_image(w: u32, h: u32) -> Vec<u8> {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut data = vec![0u8; (w * h) as usize * 4];
        for y in 0..h {
            for x in 0..w {
                let i = ((y * w + x) * 4) as usize;
                let r = next();
                if r % 7 == 0 || (x / 37 + y / 23) % 5 == 0 {
                    data[i] = (r >> 8) as u8;
                    data[i + 1] = (x % 256) as u8;
                    data[i + 2] = (y % 256) as u8;
                    data[i + 3] = 255;
                }
            }
        }
        data
    }

    fn whole_image_lanczos(data: &[u8], w: u32, h: u32, dw: u32, dh: u32) -> Vec<u8> {
        let src = fir::images::ImageRef::new(w, h, data, fir::PixelType::U8x4).unwrap();
        let mut dst = fir::images::Image::new(dw, dh, fir::PixelType::U8x4);
        let options = fir::ResizeOptions::new()
            .resize_alg(fir::ResizeAlg::Convolution(fir::FilterType::Lanczos3));
        fir::Resizer::new()
            .resize(&src, &mut dst, Some(&options))
            .unwrap();
        dst.into_vec()
    }

    fn chunked_lanczos(data: &[u8], w: u32, h: u32, dw: u32, dh: u32, chunk_src_px: u32) -> Vec<u8> {
        let mut chunker = LanczosChunker {
            data,
            src_w: w,
            src_h: h,
            chunk_src_px,
            resizer: fir::Resizer::new(),
            mul_div: fir::MulDiv::default(),
            scratch: Vec::new(),
            dst: Vec::new(),
        };
        let mut out = vec![0u8; (dw * dh) as usize * 4];
        // Bands of uneven height to also exercise band boundaries.
        let row_bytes = dw as usize * 4;
        let mut p0 = 0;
        while p0 < dh {
            let p1 = (p0 + 53).min(dh);
            let band = &mut out[p0 as usize * row_bytes..p1 as usize * row_bytes];
            chunker.fill_band(dw, dh, p0, p1, band).unwrap();
            p0 = p1;
        }
        out
    }

    /// Largest per-channel difference between two pixels in premultiplied space.
    fn premultiplied_diff(p: &[u8], q: &[u8]) -> u32 {
        let pm = |px: &[u8], i: usize| (px[i] as u32 * px[3] as u32 + 127) / 255;
        (0..3)
            .map(|i| pm(p, i).abs_diff(pm(q, i)))
            .chain([(p[3] as u32).abs_diff(q[3] as u32)])
            .max()
            .unwrap()
    }

    #[test]
    fn chunked_lanczos_matches_whole_image() {
        let (w, h) = (1200, 1385);
        let data = test_image(w, h);
        // Downscale factors spanning the pyramid (×1 … ×32), with the
        // CRS-style non-uniform vertical ratio.
        for (dw, dh) in [(1200, 1039), (600, 520), (150, 130), (38, 33)] {
            let expected = whole_image_lanczos(&data, w, h, dw, dh);
            // Small windows force many chunks, including 1-px ones.
            for chunk_src_px in [64, 257, 1000] {
                let got = chunked_lanczos(&data, w, h, dw, dh, chunk_src_px);
                // fir's u8 fixed-point precision depends on the largest
                // coefficient in the whole set, so chunks can round 1 LSB
                // differently in premultiplied space. Un-premultiplying at
                // low alpha amplifies that, so compare premultiplied values.
                let max_diff = expected
                    .chunks_exact(4)
                    .zip(got.chunks_exact(4))
                    .map(|(a, b)| premultiplied_diff(a, b))
                    .max()
                    .unwrap();
                assert!(
                    max_diff <= 2,
                    "{}×{} chunk {}: max premultiplied diff {}",
                    dw,
                    dh,
                    chunk_src_px,
                    max_diff
                );
            }
        }
    }
}
