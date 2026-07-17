//! Feature-gated (`asset-image`) image byte-step primitives for the native
//! `AssetService`: the decompression-bomb decode limits, the output-format
//! resolver, and the parameterized THUMBNAIL/RESIZE/CONVERT transform. Extracted
//! verbatim from the former god file; every item keeps its original
//! `#[cfg(feature = "asset-image")]` gate.

#[cfg(feature = "asset-image")]
use super::ByteStepParams;
#[cfg(feature = "asset-image")]
use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;

// ── image-step decode limits (decompression-bomb guard) ───────────────────────
// Both limits are enforced BEFORE the full decode: the byte cap against the
// fetched object length, and the pixel cap against the image HEADER dimensions
// (a header probe, not a full decode). Over-limit fails the step CLOSED with a
// typed reason — never a panic, never a silent pass.

/// Largest source object (in bytes) an image step will even attempt to decode.
/// 32 MiB — generous for real photos/scans, small enough to bound memory before
/// the decoder allocates.
#[cfg(feature = "asset-image")]
pub(crate) const MAX_IMAGE_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// Largest source image (width × height) an image step will decode. 64 MP caps
/// the post-decode RGBA buffer (~256 MiB at 4 B/px) and rejects pixel-flood
/// decompression bombs whose tiny encoded size passes the byte cap.
#[cfg(feature = "asset-image")]
pub(crate) const MAX_IMAGE_PIXELS: u64 = 64_000_000;

/// Default square edge for a THUMBNAIL step when no dimensions are requested.
#[cfg(feature = "asset-image")]
const DEFAULT_THUMBNAIL_EDGE: u32 = 256;

/// Reject a source object whose byte length exceeds [`MAX_IMAGE_INPUT_BYTES`].
/// Pure predicate (no decode) so it is unit-testable without the image crate.
#[cfg(feature = "asset-image")]
pub(crate) fn check_input_bytes(len: u64) -> Result<(), String> {
    if len > MAX_IMAGE_INPUT_BYTES {
        return Err(format!(
            "source image is {len} bytes, exceeds the {MAX_IMAGE_INPUT_BYTES}-byte image-step decode limit"
        ));
    }
    Ok(())
}

/// Reject a source image whose HEADER pixel count exceeds [`MAX_IMAGE_PIXELS`].
/// Pure predicate over already-probed header dimensions (no decode), so it is
/// unit-testable without the image crate.
#[cfg(feature = "asset-image")]
pub(crate) fn check_image_pixels(width: u32, height: u32) -> Result<(), String> {
    let pixels = u64::from(width) * u64::from(height);
    if pixels > MAX_IMAGE_PIXELS {
        return Err(format!(
            "source image is {width}x{height} ({pixels} px), exceeds the {MAX_IMAGE_PIXELS}-pixel image-step decode limit"
        ));
    }
    Ok(())
}

/// Resolve the requested output `format` to a concrete encoder + content-type +
/// file extension. `None` uses the per-step default. This build links only the
/// PNG and JPEG codecs (Cargo `image` feature set), so any other format fails
/// CLOSED with `invalid_argument`-grade reasoning rather than a capability lie.
#[cfg(feature = "asset-image")]
pub(crate) fn resolve_output_format(
    requested: Option<&str>,
    default: image::ImageFormat,
) -> Result<(image::ImageFormat, &'static str, &'static str), String> {
    let triple = |fmt: image::ImageFormat| match fmt {
        image::ImageFormat::Jpeg => (image::ImageFormat::Jpeg, "image/jpeg", "jpg"),
        _ => (image::ImageFormat::Png, "image/png", "png"),
    };
    match requested {
        None => Ok(triple(default)),
        Some(f) => match f {
            "png" => Ok(triple(image::ImageFormat::Png)),
            "jpg" | "jpeg" => Ok(triple(image::ImageFormat::Jpeg)),
            other => Err(format!(
                "unsupported output image format '{other}' (this build supports: png, jpeg)"
            )),
        },
    }
}

/// Apply the parameterized image transform for a byte step. Pure (decode→encode
/// happen around it in `run_byte_step`), so the param→geometry behavior is unit-
/// testable on a tiny in-memory image with no object backend or pool.
///
/// THUMBNAIL squares to [`DEFAULT_THUMBNAIL_EDGE`] unless params override the edge.
/// RESIZE honors the requested width/height (aspect-preserving, fits the box). A
/// RESIZE with no dimensions but a `format` is a CONVERT: keep the original size,
/// re-encode into the requested format. A RESIZE with neither dimensions nor a
/// format, or any non-image step type, fails EXPLICITLY (no silent fallback).
#[cfg(feature = "asset-image")]
pub(crate) fn apply_image_transform(
    img: image::DynamicImage,
    step_type: asset_entity_pb::StepType,
    params: &ByteStepParams,
) -> Result<image::DynamicImage, String> {
    use asset_entity_pb::StepType as T;
    let out = match step_type {
        T::Thumbnail => {
            let w = params.width.unwrap_or(DEFAULT_THUMBNAIL_EDGE);
            let h = params.height.unwrap_or(DEFAULT_THUMBNAIL_EDGE);
            img.thumbnail(w, h)
        }
        T::Resize => match (params.width, params.height) {
            (None, None) if params.format.is_some() => img,
            (None, None) => {
                return Err(
                    "RESIZE requires a width and/or height param (or a format param to convert only)"
                        .to_string(),
                );
            }
            (w, h) => img.resize(
                w.unwrap_or(u32::MAX),
                h.unwrap_or(u32::MAX),
                image::imageops::FilterType::Lanczos3,
            ),
        },
        other => {
            return Err(format!(
                "byte step {} is not an image transform",
                other.as_str_name()
            ));
        }
    };
    Ok(out)
}
