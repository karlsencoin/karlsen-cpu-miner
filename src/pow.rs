//! KarlsenHashV2 proof of work: blake3(pre_pow || time || 0^32 || nonce) → FishHashPlus → blake3.
//!
//! Mirrors `karlsen_pow::State::calculate_pow_khashv2plus` in rusty-karlsen.

use karlsen_math::Uint256;

use crate::fishhash::{fishhashplus_kernel_lanes, DatasetSource};

/// Lane counts the miner can run (nonces hashed together per worker iteration; see
/// `fishhash` for why). The best value depends on the CPU and is picked by `autotune`.
pub const LANE_OPTIONS: [usize; 3] = [4, 8, 16];
/// Largest entry of `LANE_OPTIONS`; nonce chunks are aligned to it.
pub const MAX_LANES: usize = 16;
#[cfg(test)]
const LANES: usize = 8;

/// Precomputed per-job state shared by all worker threads.
#[derive(Clone)]
pub struct PowJob {
    /// Blake3 state after absorbing PRE_POW_HASH || TIME || 32 zero bytes.
    prefix: blake3::Hasher,
    /// Network (block) target from the header bits.
    pub block_target: Uint256,
    /// Target a hash must reach to be reported (share target in stratum mode,
    /// equal to `block_target` in solo mode).
    pub report_target: Uint256,
}

impl PowJob {
    pub fn new(
        pre_pow_hash: &[u8; 32],
        timestamp: u64,
        block_target: Uint256,
        report_target: Uint256,
    ) -> Self {
        let mut prefix = blake3::Hasher::new();
        prefix.update(pre_pow_hash);
        prefix.update(&timestamp.to_le_bytes());
        prefix.update(&[0u8; 32]);
        Self {
            prefix,
            block_target,
            report_target,
        }
    }

    /// Computes the PoW values for `N` consecutive nonces starting at `first_nonce`.
    #[inline]
    pub fn hash_lanes_n<const N: usize>(
        &self,
        first_nonce: u64,
        source: &DatasetSource,
    ) -> [Uint256; N] {
        let seeds: [[u8; 32]; N] = std::array::from_fn(|lane| {
            let mut h = self.prefix.clone();
            h.update(&first_nonce.wrapping_add(lane as u64).to_le_bytes());
            *h.finalize().as_bytes()
        });
        let mixed = fishhashplus_kernel_lanes::<N>(&seeds, source);
        std::array::from_fn(|lane| Uint256::from_le_bytes(*blake3::hash(&mixed[lane]).as_bytes()))
    }

    #[cfg(test)]
    pub fn hash_lanes(&self, first_nonce: u64, source: &DatasetSource) -> [Uint256; LANES] {
        self.hash_lanes_n::<LANES>(first_nonce, source)
    }

    /// Single-nonce PoW, used to double-check a candidate before it is submitted.
    pub fn hash_one(&self, nonce: u64, source: &DatasetSource) -> Uint256 {
        let mut h = self.prefix.clone();
        h.update(&nonce.to_le_bytes());
        let seed = *h.finalize().as_bytes();
        let mixed = crate::fishhash::fishhashplus_kernel(&seed, source);
        Uint256::from_le_bytes(*blake3::hash(&mixed).as_bytes())
    }
}

/// Stratum share target: (2^224 − 1) / difficulty, matching karlsen-stratum-bridge's
/// `diff_to_target_standard`.
pub fn share_target_from_difficulty(difficulty: f64) -> Uint256 {
    let max_target = Uint256::from_be_bytes({
        let mut b = [0u8; 32];
        b[4..].fill(0xff);
        b
    });
    if !(difficulty.is_finite()) || difficulty <= 0.0 {
        return max_target;
    }
    // Scale like the bridge (1e18 fixed point) but in two steps so nothing overflows u128.
    let scaled = (difficulty * 1e9) as u128;
    if scaled == 0 {
        return max_target;
    }
    let numerator = max_target * Uint256::from_u64(1_000_000_000);
    numerator / Uint256::from_u128(scaled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn difficulty_one_is_max_target() {
        let t = share_target_from_difficulty(1.0);
        let mut b = [0u8; 32];
        b[4..].fill(0xff);
        assert_eq!(t, Uint256::from_be_bytes(b));
    }

    #[test]
    fn higher_difficulty_lowers_target() {
        assert!(share_target_from_difficulty(4.0) < share_target_from_difficulty(1.0));
    }
}

/// Cross-checks this crate's KarlsenHashV2 against the node's own implementation
/// (`karlsen_pow::State`), using the light cache on both sides so no 4.5 GiB dataset is needed.
#[cfg(test)]
mod node_equivalence {
    use std::sync::Arc;

    use karlsen_consensus_core::constants::BLOCK_VERSION_KHASHV2;
    use karlsen_consensus_core::hashing::header::hash_override_nonce_time;
    use karlsen_consensus_core::header::Header;
    use karlsen_hashes::pow_hashers::FishHashContext;
    use karlsen_hashes::Hash;
    use karlsen_math::{Uint192, Uint256};

    use super::*;
    use crate::fishhash::{build_light_cache, calculate_dataset_item, DatasetSource};

    #[test]
    fn matches_node_pow_and_dataset_items() {
        let header = Header::new_finalized(
            BLOCK_VERSION_KHASHV2,
            vec![vec![Hash::from_bytes([7; 32])]],
            Hash::from_bytes([1; 32]),
            Hash::from_bytes([2; 32]),
            Hash::from_bytes([3; 32]),
            1_761_000_000_123,
            0x1d0f_ffff,
            0,
            93_849_111,
            Uint192::from_u64(123_456),
            42,
            Hash::from_bytes([4; 32]),
        );
        let node_context = Arc::new(FishHashContext::new(false, None));
        let node_state = karlsen_pow::State::new(&header, node_context);

        let light_cache = build_light_cache();
        let source = DatasetSource::Light(&light_cache);
        let pre_pow = hash_override_nonce_time(&header, 0, 0).as_bytes();
        let job = PowJob::new(&pre_pow, header.timestamp, Uint256::ZERO, Uint256::ZERO);

        // Lane path and single path both match the node for a spread of nonces.
        let base = 0x0123_4567_89ab_cde0u64;
        let lanes = job.hash_lanes(base, &source);
        for (lane, value) in lanes.iter().enumerate() {
            let nonce = base + lane as u64;
            assert_eq!(
                *value,
                node_state.calculate_pow(nonce),
                "lane {lane} differs from node"
            );
            assert_eq!(job.hash_one(nonce, &source), *value);
        }

        // Every lane count the autotuner may pick gives the same hashes.
        let four = job.hash_lanes_n::<4>(base, &source);
        let sixteen = job.hash_lanes_n::<16>(base, &source);
        assert_eq!(&four[..], &lanes[..4]);
        assert_eq!(&sixteen[..8], &lanes[..]);
        for (lane, value) in sixteen.iter().enumerate().skip(8) {
            assert_eq!(
                *value,
                node_state.calculate_pow(base + lane as u64),
                "16-lane path lane {lane} differs from node"
            );
        }

        // A full-dataset buffer built by this crate gives the same results as light mode.
        let indices = [0usize, 1, 12_345, 37_748_716];
        for &i in &indices {
            let a = calculate_dataset_item(&light_cache, i);
            let b = karlsen_hashes::pow_hashers::calculate_dataset_item_1024(
                node_light_cache_view(&light_cache),
                i,
            );
            let b_words: Vec<u32> = karlsen_hashes::pow_hashers::HashData::as_bytes(&b)
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect();
            assert_eq!(a.to_vec(), b_words, "dataset item {i} differs from node");
        }
    }

    /// Reinterprets our light cache as the node's `Hash512` slice (both are 64 plain bytes,
    /// little-endian words on every supported target).
    fn node_light_cache_view(
        cache: &[crate::fishhash::LightItem],
    ) -> &[karlsen_hashes::pow_hashers::Hash512] {
        assert!(cfg!(target_endian = "little"));
        assert_eq!(
            std::mem::size_of::<karlsen_hashes::pow_hashers::Hash512>(),
            64
        );
        unsafe {
            std::slice::from_raw_parts(
                cache.as_ptr() as *const karlsen_hashes::pow_hashers::Hash512,
                cache.len(),
            )
        }
    }
}
