//! FishHashPlus implementation tuned for CPUs.
//!
//! This is a re-implementation of `karlsen_hashes::PowFishHash::fishhashplus_kernel`
//! (rusty-karlsen `crypto/hashes/src/pow_hashers.rs`) with three CPU-specific changes:
//!
//! 1. The full dataset is a flat byte slice so it can live in anonymous memory
//!    (with transparent huge pages) or in a memory-mapped file shared between processes.
//! 2. Several nonces are processed in lock-step ("lanes") and their dataset reads are
//!    prefetched together, so the CPU keeps many DRAM requests in flight instead of
//!    stalling on one random 128-byte read at a time.
//! 3. A light mode computes dataset items on demand from the ~72 MiB light cache. It is
//!    roughly a thousand times slower and exists only for verification and very small devices.
//!
//! Correctness is checked in `tests` against the node's own implementation.

use std::sync::atomic::{AtomicUsize, Ordering};

pub const FNV_PRIME: u32 = 0x0100_0193;
pub const FULL_DATASET_ITEM_PARENTS: u32 = 512;
pub const NUM_DATASET_ACCESSES: u32 = 32;
pub const LIGHT_CACHE_ROUNDS: usize = 3;
pub const LIGHT_CACHE_NUM_ITEMS: u32 = 1_179_641;
pub const FULL_DATASET_NUM_ITEMS: u32 = 37_748_717;
pub const DATASET_ITEM_BYTES: usize = 128;
pub const LIGHT_ITEM_BYTES: usize = 64;
/// Size of the full dataset in bytes (37,748,717 × 128 = 4,831,835,776 bytes).
pub const FULL_DATASET_BYTES: usize = FULL_DATASET_NUM_ITEMS as usize * DATASET_ITEM_BYTES;

/// FishHash seed used by the Karlsen node (the specification fixes it).
#[rustfmt::skip]
pub const SEED: [u8; 32] = [
    0xeb, 0x01, 0x63, 0xae, 0xf2, 0xab, 0x1c, 0x5a,
    0x66, 0x31, 0x0c, 0x1c, 0x14, 0xd6, 0x0f, 0x42,
    0x55, 0xa9, 0xb3, 0x9b, 0x0e, 0xdf, 0x26, 0x53,
    0x98, 0x44, 0xf1, 0x17, 0xad, 0x67, 0x21, 0x19,
];

/// One dataset item: 1024 bits viewed as 32 little-endian u32 words.
pub type Item = [u32; 32];
/// One light cache item: 512 bits viewed as 16 little-endian u32 words.
pub type LightItem = [u32; 16];

#[inline(always)]
pub fn fnv1(u: u32, v: u32) -> u32 {
    u.wrapping_mul(FNV_PRIME) ^ v
}

#[inline(always)]
fn keccak512_in_place(words: &mut LightItem) {
    let mut bytes = [0u8; LIGHT_ITEM_BYTES];
    for (chunk, w) in bytes.chunks_exact_mut(4).zip(words.iter()) {
        chunk.copy_from_slice(&w.to_le_bytes());
    }
    let mut hasher = tiny_keccak::Keccak::v512();
    tiny_keccak::Hasher::update(&mut hasher, &bytes);
    tiny_keccak::Hasher::finalize(hasher, &mut bytes);
    for (chunk, w) in bytes.chunks_exact(4).zip(words.iter_mut()) {
        *w = u32::from_le_bytes(chunk.try_into().unwrap());
    }
}

/// Builds the ~72 MiB light cache exactly like the node does.
pub fn build_light_cache() -> Vec<LightItem> {
    let n = LIGHT_CACHE_NUM_ITEMS as usize;
    let mut cache = vec![[0u32; 16]; n];

    let mut item_bytes = [0u8; LIGHT_ITEM_BYTES];
    {
        let mut hasher = tiny_keccak::Keccak::v512();
        tiny_keccak::Hasher::update(&mut hasher, &SEED);
        tiny_keccak::Hasher::finalize(hasher, &mut item_bytes);
    }
    let mut item = [0u32; 16];
    for (chunk, w) in item_bytes.chunks_exact(4).zip(item.iter_mut()) {
        *w = u32::from_le_bytes(chunk.try_into().unwrap());
    }
    cache[0] = item;
    for slot in cache.iter_mut().skip(1) {
        keccak512_in_place(&mut item);
        *slot = item;
    }

    for _ in 0..LIGHT_CACHE_ROUNDS {
        for i in 0..LIGHT_CACHE_NUM_ITEMS {
            let v = cache[i as usize][0] % LIGHT_CACHE_NUM_ITEMS;
            let w = LIGHT_CACHE_NUM_ITEMS.wrapping_add(i.wrapping_sub(1)) % LIGHT_CACHE_NUM_ITEMS;
            let mut x = [0u32; 16];
            for k in 0..16 {
                x[k] = cache[v as usize][k] ^ cache[w as usize][k];
            }
            keccak512_in_place(&mut x);
            cache[i as usize] = x;
        }
    }
    cache
}

/// Computes one full dataset item from the light cache (512 parents per half).
pub fn calculate_dataset_item(light_cache: &[LightItem], index: usize) -> Item {
    let seed0 = (index * 2) as u32;
    let seed1 = seed0.wrapping_add(1);

    let mut mix0 = light_cache[(seed0 % LIGHT_CACHE_NUM_ITEMS) as usize];
    let mut mix1 = light_cache[(seed1 % LIGHT_CACHE_NUM_ITEMS) as usize];
    mix0[0] ^= seed0;
    mix1[0] ^= seed1;
    keccak512_in_place(&mut mix0);
    keccak512_in_place(&mut mix1);

    for j in 0..FULL_DATASET_ITEM_PARENTS {
        let t0 = fnv1(seed0 ^ j, mix0[(j % 16) as usize]);
        let t1 = fnv1(seed1 ^ j, mix1[(j % 16) as usize]);
        let p0 = &light_cache[(t0 % LIGHT_CACHE_NUM_ITEMS) as usize];
        let p1 = &light_cache[(t1 % LIGHT_CACHE_NUM_ITEMS) as usize];
        for k in 0..16 {
            mix0[k] = fnv1(mix0[k], p0[k]);
            mix1[k] = fnv1(mix1[k], p1[k]);
        }
    }

    keccak512_in_place(&mut mix0);
    keccak512_in_place(&mut mix1);

    let mut out = [0u32; 32];
    out[..16].copy_from_slice(&mix0);
    out[16..].copy_from_slice(&mix1);
    out
}

/// Fills `dst` (a byte slice holding consecutive dataset items starting at `first_item`)
/// using `threads` worker threads. `progress` counts finished items.
pub fn fill_dataset(dst: &mut [u8], first_item: usize, light_cache: &[LightItem], threads: usize, progress: &AtomicUsize) {
    assert_eq!(dst.len() % DATASET_ITEM_BYTES, 0);
    let items = dst.len() / DATASET_ITEM_BYTES;
    let threads = threads.max(1);
    let per_thread = items.div_ceil(threads).max(1);

    std::thread::scope(|scope| {
        for (chunk_index, chunk) in dst.chunks_mut(per_thread * DATASET_ITEM_BYTES).enumerate() {
            let start = first_item + chunk_index * per_thread;
            scope.spawn(move || {
                for (offset, out) in chunk.chunks_exact_mut(DATASET_ITEM_BYTES).enumerate() {
                    let item = calculate_dataset_item(light_cache, start + offset);
                    for (b, w) in out.chunks_exact_mut(4).zip(item.iter()) {
                        b.copy_from_slice(&w.to_le_bytes());
                    }
                    if offset % 4096 == 4095 {
                        progress.fetch_add(4096, Ordering::Relaxed);
                    }
                }
                progress.fetch_add(chunk.len() / DATASET_ITEM_BYTES % 4096, Ordering::Relaxed);
            });
        }
    });
}

/// Where dataset items come from while hashing.
pub enum DatasetSource<'a> {
    /// Full precomputed dataset, `FULL_DATASET_BYTES` long.
    Full(&'a [u8]),
    /// Items computed on demand from the light cache.
    Light(&'a [LightItem]),
}

impl DatasetSource<'_> {
    #[inline(always)]
    fn prefetch(&self, index: u32) {
        if let DatasetSource::Full(data) = self {
            let ptr = unsafe { data.as_ptr().add(index as usize * DATASET_ITEM_BYTES) };
            prefetch_item(ptr);
        }
    }

    #[inline(always)]
    fn load(&self, index: u32) -> Item {
        match self {
            DatasetSource::Full(data) => {
                let start = index as usize * DATASET_ITEM_BYTES;
                let bytes = &data[start..start + DATASET_ITEM_BYTES];
                let mut out = [0u32; 32];
                for (w, chunk) in out.iter_mut().zip(bytes.chunks_exact(4)) {
                    *w = u32::from_le_bytes(chunk.try_into().unwrap());
                }
                out
            }
            DatasetSource::Light(cache) => calculate_dataset_item(cache, index as usize),
        }
    }
}

/// Hints the CPU to pull both 64-byte cache lines of a dataset item.
#[inline(always)]
fn prefetch_item(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
        _mm_prefetch(ptr.add(64) as *const i8, _MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            "prfm pldl1keep, [{0}, #64]",
            in(reg) ptr,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}

#[inline(always)]
fn dataset_indices(mix: &Item, round: u32) -> [u32; 3] {
    let mut group = [0u32; 8];
    for (c, g) in group.iter_mut().enumerate() {
        *g = mix[4 * c] ^ mix[4 * c + 1] ^ mix[4 * c + 2] ^ mix[4 * c + 3];
    }
    [
        (group[0] ^ group[3] ^ group[6]) % FULL_DATASET_NUM_ITEMS,
        (group[1] ^ group[4] ^ group[7]) % FULL_DATASET_NUM_ITEMS,
        (group[2] ^ group[5] ^ round) % FULL_DATASET_NUM_ITEMS,
    ]
}

#[inline(always)]
fn mix_round(mix: &mut Item, fetch0: &Item, fetch1: &Item, fetch2: &Item) {
    let mut f1 = [0u32; 32];
    let mut f2 = [0u32; 32];
    for j in 0..32 {
        f1[j] = fnv1(mix[j], fetch1[j]);
        f2[j] = mix[j] ^ fetch2[j];
    }
    for j in 0..16 {
        let a = fetch0[2 * j] as u64 | (fetch0[2 * j + 1] as u64) << 32;
        let b = f1[2 * j] as u64 | (f1[2 * j + 1] as u64) << 32;
        let c = f2[2 * j] as u64 | (f2[2 * j + 1] as u64) << 32;
        let r = a.wrapping_mul(b).wrapping_add(c);
        mix[2 * j] = r as u32;
        mix[2 * j + 1] = (r >> 32) as u32;
    }
}

#[inline(always)]
fn seed_mix(seed: &[u8; 32]) -> Item {
    // Hash1024::from_512s(seed512, seed512) where seed512 = seed || 32 zero bytes.
    let mut mix = [0u32; 32];
    for (i, chunk) in seed.chunks_exact(4).enumerate() {
        let w = u32::from_le_bytes(chunk.try_into().unwrap());
        mix[i] = w;
        mix[16 + i] = w;
    }
    mix
}

#[inline(always)]
fn collapse(mix: &Item) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in (0..32).step_by(4) {
        let h = fnv1(fnv1(fnv1(mix[i], mix[i + 1]), mix[i + 2]), mix[i + 3]);
        out[i..i + 4].copy_from_slice(&h.to_le_bytes());
    }
    out
}

/// FishHashPlus kernel for `N` independent seeds processed in lock-step.
///
/// Every round first computes all lanes' indices and prefetches their 3·N items, then
/// mixes. With N = 4..8 a single core keeps 12..24 DRAM reads in flight.
#[inline(always)]
pub fn fishhashplus_kernel_lanes<const N: usize>(seeds: &[[u8; 32]; N], source: &DatasetSource) -> [[u8; 32]; N] {
    let mut mixes: [Item; N] = std::array::from_fn(|lane| seed_mix(&seeds[lane]));
    let mut indices = [[0u32; 3]; N];

    for round in 0..NUM_DATASET_ACCESSES {
        for lane in 0..N {
            indices[lane] = dataset_indices(&mixes[lane], round);
            for &idx in &indices[lane] {
                source.prefetch(idx);
            }
        }
        for lane in 0..N {
            let [p0, p1, p2] = indices[lane];
            let fetch0 = source.load(p0);
            let fetch1 = source.load(p1);
            let fetch2 = source.load(p2);
            mix_round(&mut mixes[lane], &fetch0, &fetch1, &fetch2);
        }
    }

    std::array::from_fn(|lane| collapse(&mixes[lane]))
}

/// Single-seed convenience wrapper (used by tests and verification paths).
pub fn fishhashplus_kernel(seed: &[u8; 32], source: &DatasetSource) -> [u8; 32] {
    fishhashplus_kernel_lanes::<1>(&[*seed], source)[0]
}
