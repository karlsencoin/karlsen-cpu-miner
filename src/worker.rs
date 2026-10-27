//! Mining threads: each thread pins itself to a core, takes nonce chunks from the
//! current work unit and reports hashes that meet the report target.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use karlsen_math::Uint256;
use log::{debug, info, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::dataset::Dataset;
use crate::pow::{PowJob, LANES};

/// Nonces handed to a thread per atomic fetch. Large enough to keep the shared counter
/// cold, small enough that a new job is picked up within a few milliseconds.
const NONCE_CHUNK: u64 = (LANES as u64) * 32;

/// What a work unit came from, so solutions can be routed back.
pub enum WorkOrigin {
    /// Solo mining: the block template as received from GetBlockTemplate.
    Node { template: karlsen_rpc_core::RpcRawBlock },
    /// Pool / bridge mining: stratum job id, echoed back exactly as the server sent it
    /// (some servers use strings, others numbers).
    Stratum { job_id: serde_json::Value },
}

pub struct WorkUnit {
    pub generation: u64,
    pub pow: PowJob,
    pub origin: WorkOrigin,
    /// Fixed high nonce bits (stratum extranonce); zero in solo mode.
    nonce_prefix: u64,
    /// Bits the miner may vary.
    nonce_free_mask: u64,
    next_nonce: AtomicU64,
}

impl WorkUnit {
    pub fn new(generation: u64, pow: PowJob, origin: WorkOrigin, nonce_prefix: u64, nonce_free_mask: u64) -> Self {
        // Random start so several miners on one address do not repeat each other's work.
        // Aligned to LANES so a lane group never carries into the fixed prefix bits.
        let start = rand::random::<u64>() & nonce_free_mask & !(LANES as u64 - 1);
        Self { generation, pow, origin, nonce_prefix, nonce_free_mask, next_nonce: AtomicU64::new(start) }
    }

    #[inline]
    fn take_chunk(&self) -> u64 {
        self.next_nonce.fetch_add(NONCE_CHUNK, Ordering::Relaxed)
    }

    #[inline]
    fn nonce(&self, counter: u64) -> u64 {
        self.nonce_prefix | (counter & self.nonce_free_mask)
    }
}

pub struct Solution {
    pub unit: Arc<WorkUnit>,
    pub nonce: u64,
    pub pow_value: Uint256,
    pub meets_block_target: bool,
}

/// The current work unit, swapped atomically by the network client.
#[derive(Default)]
pub struct WorkSlot {
    current: RwLock<Option<Arc<WorkUnit>>>,
    generation: AtomicU64,
    paused: AtomicBool,
}

impl WorkSlot {
    pub fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn publish(&self, unit: WorkUnit) {
        *self.current.write().unwrap() = Some(Arc::new(unit));
        self.paused.store(false, Ordering::Release);
    }

    /// Stops hashing until the next `publish` (e.g. the connection dropped or the node is not synced).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        *self.current.write().unwrap() = None;
    }

    fn snapshot(&self) -> Option<Arc<WorkUnit>> {
        if self.paused.load(Ordering::Acquire) {
            return None;
        }
        self.current.read().unwrap().clone()
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

pub struct WorkerConfig {
    pub threads: usize,
    pub pin_threads: bool,
}

pub struct Stats {
    pub hashes: AtomicU64,
}

pub fn spawn_workers(
    config: &WorkerConfig,
    dataset: Arc<Dataset>,
    slot: Arc<WorkSlot>,
    solutions: UnboundedSender<Solution>,
    stats: Arc<Stats>,
    shutdown: Arc<AtomicBool>,
) -> Vec<std::thread::JoinHandle<()>> {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    if config.pin_threads && cores.len() < config.threads {
        warn!("{} threads requested but only {} cores reported; some threads will share cores", config.threads, cores.len());
    }

    (0..config.threads)
        .map(|index| {
            let dataset = dataset.clone();
            let slot = slot.clone();
            let solutions = solutions.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();
            let core = if config.pin_threads && !cores.is_empty() { Some(cores[index % cores.len()]) } else { None };
            std::thread::Builder::new()
                .name(format!("karlsen-cpu-miner-{index}"))
                .spawn(move || {
                    if let Some(core) = core {
                        if !core_affinity::set_for_current(core) {
                            debug!("worker {index}: could not pin to core {:?}", core);
                        }
                    }
                    worker_loop(index, &dataset, &slot, &solutions, &stats, &shutdown);
                })
                .expect("spawning mining thread")
        })
        .collect()
}

fn worker_loop(
    index: usize,
    dataset: &Dataset,
    slot: &WorkSlot,
    solutions: &UnboundedSender<Solution>,
    stats: &Stats,
    shutdown: &AtomicBool,
) {
    let source = dataset.source();
    debug!("worker {index} started");

    while !shutdown.load(Ordering::Relaxed) {
        let Some(unit) = slot.snapshot() else {
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        };

        // Hash this unit until a newer generation is published.
        while slot.current_generation() == unit.generation && !shutdown.load(Ordering::Relaxed) {
            let chunk_start = unit.take_chunk();
            let mut offset = 0;
            while offset < NONCE_CHUNK {
                let first = unit.nonce(chunk_start.wrapping_add(offset));
                let values = unit.pow.hash_lanes(first, &source);
                for (lane, value) in values.iter().enumerate() {
                    if *value <= unit.pow.report_target {
                        let nonce = first.wrapping_add(lane as u64);
                        let meets_block_target = *value <= unit.pow.block_target;
                        if meets_block_target {
                            info!("Block candidate found by thread {index}: nonce {nonce:#018x}");
                        }
                        let _ = solutions.send(Solution { unit: unit.clone(), nonce, pow_value: *value, meets_block_target });
                    }
                }
                offset += LANES as u64;
                stats.hashes.fetch_add(LANES as u64, Ordering::Relaxed);
            }
        }
    }
    debug!("worker {index} stopped");
}
