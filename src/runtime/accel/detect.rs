//! Cached runtime CPU-feature detection (D.5).
//!
//! Feature detection (`is_x86_feature_detected!` / `is_aarch64_feature_detected!`)
//! is probed ONCE per process and cached, so the accel wrappers can pick scalar
//! vs SIMD on every call without re-running the (relatively costly) probe.

use std::sync::OnceLock;

/// SIMD-relevant CPU features, detected once. All `false` on unknown targets
/// (scalar fallback always works).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CpuFeatures {
    pub sse42: bool,
    pub avx2: bool,
    pub neon: bool,
}

/// The process-wide cached feature set.
pub fn cpu_features() -> CpuFeatures {
    static FEATURES: OnceLock<CpuFeatures> = OnceLock::new();
    *FEATURES.get_or_init(detect)
}

fn detect() -> CpuFeatures {
    #[cfg(target_arch = "x86_64")]
    {
        CpuFeatures {
            sse42: std::arch::is_x86_feature_detected!("sse4.2"),
            avx2: std::arch::is_x86_feature_detected!("avx2"),
            neon: false,
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        CpuFeatures {
            sse42: false,
            avx2: false,
            neon: std::arch::is_aarch64_feature_detected!("neon"),
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        CpuFeatures::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_features_are_cached_and_stable() {
        let a = cpu_features();
        let b = cpu_features();
        assert_eq!(a, b, "detection must be cached (identical across calls)");
    }
}
