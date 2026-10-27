//! Owns the FishHashPlus dataset for the lifetime of the miner.
//!
//! The FishHashPlus seed is fixed, so the 4.5 GiB dataset never changes. That allows two
//! storage modes besides the classic "build it in RAM on every start":
//!
//! * `memory` — anonymous memory, built at start-up (about one minute on 8 cores),
//!   marked for transparent huge pages on Linux to cut TLB misses.
//! * `file`   — built once into a file, then memory-mapped on every later start. Restarts
//!   are instant and several miner processes on the same machine share one copy
//!   through the page cache.
//! * `light`  — no dataset at all; items are derived from the light cache per access.
//!   Only useful for testing; expect a few hashes per second per core.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::memory::DatasetMemory;
use anyhow::{bail, Context, Result};
use log::{info, warn};

use crate::fishhash::{
    build_light_cache, calculate_dataset_item, fill_dataset, DatasetSource, LightItem,
    DATASET_ITEM_BYTES, FULL_DATASET_BYTES, FULL_DATASET_NUM_ITEMS,
};

/// File header: magic, format version, item count, item size. Data starts at `FILE_DATA_OFFSET`
/// so it stays page aligned.
const FILE_MAGIC: &[u8; 8] = b"KLSFHP01";
const FILE_DATA_OFFSET: usize = 4096;
/// Number of items re-derived from the light cache to validate a dataset file on load.
const VERIFY_SAMPLES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum DatasetMode {
    /// Build the full dataset in RAM at start-up.
    Memory,
    /// Build the full dataset into a file once, memory-map it afterwards.
    File,
    /// No dataset; compute items on demand (very slow, for testing).
    Light,
}

enum Storage {
    Ram(DatasetMemory),
    Light,
}

pub struct Dataset {
    light_cache: Vec<LightItem>,
    storage: Storage,
}

impl Dataset {
    pub fn load(
        mode: DatasetMode,
        file_path: &Path,
        build_threads: usize,
        large_pages: bool,
    ) -> Result<Arc<Self>> {
        let started = Instant::now();
        info!("Building FishHashPlus light cache (72 MiB)…");
        let light_cache = build_light_cache();
        info!(
            "Light cache ready in {:.1}s",
            started.elapsed().as_secs_f64()
        );

        let storage = match mode {
            DatasetMode::Light => {
                warn!(
                    "Light dataset mode: hashrate will be extremely low. Use it only for testing."
                );
                Storage::Light
            }
            DatasetMode::Memory => {
                Storage::Ram(build_anonymous(&light_cache, build_threads, large_pages)?)
            }
            DatasetMode::File => Storage::Ram(
                open_or_build_file(file_path, &light_cache, build_threads, large_pages)
                    .with_context(|| format!("dataset file {}", file_path.display()))?,
            ),
        };
        Ok(Arc::new(Self {
            light_cache,
            storage,
        }))
    }

    pub fn source(&self) -> DatasetSource<'_> {
        match &self.storage {
            Storage::Ram(mem) => DatasetSource::Full(mem.as_slice()),
            Storage::Light => DatasetSource::Light(&self.light_cache),
        }
    }
}

pub fn default_dataset_path() -> PathBuf {
    // Windows: %LOCALAPPDATA% only. HOME is often set by Git/MSYS to a Unix-style path
    // ("/c/Users/...") or an unmapped drive that Windows cannot create directories in.
    #[cfg(windows)]
    let base = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute() && p.exists())
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|e| e.parent().map(|d| d.to_path_buf()))
        })
        .unwrap_or_else(|| PathBuf::from("."));
    #[cfg(not(windows))]
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("karlsen-cpu-miner")
        .join("fishhash-plus-dataset.bin")
}

fn human_gib(bytes: usize) -> String {
    format!(
        "{:.2} GiB ({} bytes)",
        bytes as f64 / (1u64 << 30) as f64,
        bytes
    )
}

fn spawn_progress_logger(
    progress: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let total = FULL_DATASET_NUM_ITEMS as usize;
        let started = Instant::now();
        while !done.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(5));
            let finished = progress.load(Ordering::Relaxed).min(total);
            let elapsed = started.elapsed().as_secs_f64();
            let rate = finished as f64 / elapsed.max(0.001);
            let eta = if rate > 0.0 {
                (total - finished) as f64 / rate
            } else {
                0.0
            };
            info!(
                "Building FishHashPlus dataset: {:.1}% ({}/{} items), ETA {:.0}s",
                finished as f64 * 100.0 / total as f64,
                finished,
                total,
                eta
            );
        }
    })
}

fn build_into(dst: &mut [u8], light_cache: &[LightItem], threads: usize) {
    let progress = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let logger = spawn_progress_logger(progress.clone(), done.clone());
    let started = Instant::now();
    fill_dataset(dst, 0, light_cache, threads, &progress);
    done.store(true, Ordering::Relaxed);
    let _ = logger.join();
    info!(
        "FishHashPlus dataset built in {:.1}s using {} threads",
        started.elapsed().as_secs_f64(),
        threads
    );
}

fn build_anonymous(
    light_cache: &[LightItem],
    threads: usize,
    large_pages: bool,
) -> Result<DatasetMemory> {
    info!(
        "Allocating {} for the FishHashPlus dataset in RAM",
        human_gib(FULL_DATASET_BYTES)
    );
    let mut map = DatasetMemory::allocate(FULL_DATASET_BYTES, large_pages)?;
    build_into(map.as_mut_slice(), light_cache, threads);
    Ok(map)
}

fn write_header(file: &mut File) -> Result<()> {
    let mut header = vec![0u8; FILE_DATA_OFFSET];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..12].copy_from_slice(&1u32.to_le_bytes());
    header[12..16].copy_from_slice(&FULL_DATASET_NUM_ITEMS.to_le_bytes());
    header[16..20].copy_from_slice(&(DATASET_ITEM_BYTES as u32).to_le_bytes());
    file.write_all(&header)?;
    Ok(())
}

fn header_is_valid(path: &Path) -> Result<bool> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 20];
    if file.read_exact(&mut header).is_err() {
        return Ok(false);
    }
    let expected_len = (FILE_DATA_OFFSET + FULL_DATASET_BYTES) as u64;
    Ok(&header[..8] == FILE_MAGIC
        && u32::from_le_bytes(header[12..16].try_into().unwrap()) == FULL_DATASET_NUM_ITEMS
        && u32::from_le_bytes(header[16..20].try_into().unwrap()) == DATASET_ITEM_BYTES as u32
        && file.metadata()?.len() == expected_len)
}

/// Re-derives a spread of items from the light cache and compares them byte for byte.
fn verify_samples(data: &[u8], light_cache: &[LightItem]) -> bool {
    let total = FULL_DATASET_NUM_ITEMS as usize;
    (0..VERIFY_SAMPLES).all(|i| {
        let index = if i == VERIFY_SAMPLES - 1 {
            total - 1
        } else {
            i * (total / VERIFY_SAMPLES) + i * 7919 % 1000
        };
        let expected = calculate_dataset_item(light_cache, index);
        let start = index * DATASET_ITEM_BYTES;
        data[start..start + DATASET_ITEM_BYTES]
            .chunks_exact(4)
            .zip(expected.iter())
            .all(|(b, w)| b == w.to_le_bytes())
    })
}

/// Reads the dataset file into RAM with a progress log.
///
/// The dataset is read with random 128-byte accesses, so it must be fully resident: a lazily
/// faulted memory map would hit the disk on almost every hash (Windows does not pre-fault
/// mappings), which drops the hashrate to near zero.
fn load_file_into_memory(path: &Path, large_pages: bool) -> Result<DatasetMemory> {
    let started = Instant::now();
    info!(
        "Loading dataset file into RAM: {}",
        human_gib(FULL_DATASET_BYTES)
    );
    let mut map = DatasetMemory::allocate(FULL_DATASET_BYTES, large_pages)?;
    let mut file = File::open(path)?;
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(FILE_DATA_OFFSET as u64))?;
    const CHUNK: usize = 64 << 20;
    let mut last_log = Instant::now();
    let mut done = 0usize;
    for chunk in map.as_mut_slice().chunks_mut(CHUNK) {
        file.read_exact(chunk)
            .with_context(|| format!("reading {}", path.display()))?;
        done += chunk.len();
        if last_log.elapsed() > Duration::from_secs(5) {
            info!(
                "Loading dataset file: {:.1}%",
                done as f64 * 100.0 / FULL_DATASET_BYTES as f64
            );
            last_log = Instant::now();
        }
    }
    info!(
        "Dataset loaded into RAM in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(map)
}

fn open_or_build_file(
    path: &Path,
    light_cache: &[LightItem],
    threads: usize,
    large_pages: bool,
) -> Result<DatasetMemory> {
    if path.exists() && header_is_valid(path)? {
        let map = load_file_into_memory(path, large_pages)?;
        if verify_samples(map.as_slice(), light_cache) {
            info!(
                "Dataset file verified ({} sampled items match)",
                VERIFY_SAMPLES
            );
            return Ok(map);
        }
        warn!(
            "Dataset file {} failed verification; rebuilding it",
            path.display()
        );
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // Build in RAM, then save a copy to disk for the next start.
    let mut map = DatasetMemory::allocate(FULL_DATASET_BYTES, large_pages)?;
    build_into(map.as_mut_slice(), light_cache, threads);
    if !verify_samples(map.as_slice(), light_cache) {
        bail!("freshly built dataset failed verification");
    }
    info!(
        "Saving dataset to {} — {}",
        path.display(),
        human_gib(FILE_DATA_OFFSET + FULL_DATASET_BYTES)
    );
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    write_header(&mut file)?;
    file.write_all(map.as_slice())
        .with_context(|| format!("writing {}", path.display()))?;
    file.sync_all()?;
    info!("Dataset saved; later starts load it instead of rebuilding");
    Ok(map)
}
