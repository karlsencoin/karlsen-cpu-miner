//! karlsen-cpu-miner — CPU miner for Karlsen (KarlsenHashV2 / FishHashPlus).
//!
//! Two connection modes:
//! * `--node grpc://host:42110`         solo mining against karlsend, mass-preserving submission
//! * `--stratum stratum+tcp://host:port` mining through karlsen-stratum-bridge or a pool

mod dataset;
mod fishhash;
mod node_client;
mod pow;
mod stratum_client;
mod worker;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::Parser;
use karlsen_addresses::Address;
use log::info;

use dataset::{default_dataset_path, Dataset, DatasetMode};
use worker::{spawn_workers, Stats, WorkSlot, WorkerConfig};

#[derive(Parser, Debug)]
#[command(name = "karlsen-cpu-miner", version, about = "CPU miner for Karlsen (KarlsenHashV2 / FishHashPlus)")]
struct Args {
    /// Karlsen address that receives the rewards (karlsen:…).
    #[arg(long, short = 'a')]
    mining_address: String,

    /// Solo mining: karlsend gRPC endpoint, e.g. grpc://127.0.0.1:42110.
    #[arg(long, conflicts_with = "stratum")]
    node: Option<String>,

    /// Pool / bridge mining: stratum endpoint, e.g. stratum+tcp://127.0.0.1:5555.
    #[arg(long, conflicts_with = "node")]
    stratum: Option<String>,

    /// Worker name sent to the stratum server.
    #[arg(long, default_value = "karlsen-cpu-miner")]
    worker_name: String,

    /// Mining threads (default: all logical CPUs).
    #[arg(long, short = 't')]
    threads: Option<usize>,

    /// Do not pin mining threads to CPU cores.
    #[arg(long)]
    no_pin: bool,

    /// Where the 4.5 GiB FishHashPlus dataset lives.
    #[arg(long, value_enum, default_value_t = DatasetMode::Memory)]
    dataset_mode: DatasetMode,

    /// Dataset file used by `--dataset-mode file`.
    #[arg(long)]
    dataset_file: Option<PathBuf>,

    /// Threads used to build the dataset (default: all logical CPUs).
    #[arg(long)]
    build_threads: Option<usize>,

    /// Solo mode: GetBlockTemplate polling interval in milliseconds.
    #[arg(long, default_value_t = 250)]
    poll_interval_ms: u64,

    /// Solo mode: keep mining when the node reports it is not synced (testnets only).
    #[arg(long)]
    mine_when_not_synced: bool,

    /// Run a hashrate benchmark for this many seconds instead of mining.
    #[arg(long)]
    benchmark: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).format_timestamp_secs().init();
    let args = Args::parse();

    let mining_address = Address::try_from(args.mining_address.as_str())
        .map_err(|e| anyhow::anyhow!("invalid mining address '{}': {e}", args.mining_address))?;
    if args.benchmark.is_none() && args.node.is_none() && args.stratum.is_none() {
        bail!("choose a connection: --node grpc://host:42110 (solo) or --stratum stratum+tcp://host:port (bridge/pool)");
    }

    let logical = num_cpus::get();
    let threads = args.threads.unwrap_or(logical).max(1);
    let build_threads = args.build_threads.unwrap_or(logical).max(1);
    info!("karlsen-cpu-miner {} · {} mining threads · dataset mode {:?}", env!("CARGO_PKG_VERSION"), threads, args.dataset_mode);

    let dataset_path = args.dataset_file.clone().unwrap_or_else(default_dataset_path);
    let dataset = tokio::task::spawn_blocking(move || Dataset::load(args.dataset_mode, &dataset_path, build_threads)).await??;

    let slot = Arc::new(WorkSlot::default());
    let stats = Arc::new(Stats { hashes: AtomicU64::new(0) });
    let shutdown = Arc::new(AtomicBool::new(false));
    let (solution_tx, solution_rx) = tokio::sync::mpsc::unbounded_channel();

    let workers = spawn_workers(
        &WorkerConfig { threads, pin_threads: !args.no_pin },
        dataset.clone(),
        slot.clone(),
        solution_tx,
        stats.clone(),
        shutdown.clone(),
    );
    spawn_hashrate_reporter(stats.clone(), shutdown.clone());

    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutting down…");
            shutdown.store(true, Ordering::Relaxed);
        });
    }

    let result = if let Some(seconds) = args.benchmark {
        run_benchmark(&slot, &stats, seconds).await;
        shutdown.store(true, Ordering::Relaxed);
        Ok(())
    } else if let Some(node) = args.node {
        let extra = format!("karlsen-cpu-miner/{}", env!("CARGO_PKG_VERSION")).into_bytes();
        node_client::run(
            node_client::NodeConfig {
                url: node,
                mining_address,
                extra_data: extra,
                poll_interval: Duration::from_millis(args.poll_interval_ms),
                mine_when_not_synced: args.mine_when_not_synced,
            },
            dataset.clone(),
            slot.clone(),
            solution_rx,
            shutdown.clone(),
        )
        .await
    } else {
        stratum_client::run(
            stratum_client::StratumConfig {
                url: args.stratum.unwrap(),
                mining_address: args.mining_address.clone(),
                worker_name: args.worker_name.clone(),
            },
            slot.clone(),
            solution_rx,
            shutdown.clone(),
        )
        .await
    };

    shutdown.store(true, Ordering::Relaxed);
    for handle in workers {
        let _ = handle.join();
    }
    result
}

fn spawn_hashrate_reporter(stats: Arc<Stats>, shutdown: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let mut last_count = 0u64;
        let mut last_time = Instant::now();
        while !shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(30));
            let count = stats.hashes.load(Ordering::Relaxed);
            let elapsed = last_time.elapsed().as_secs_f64();
            info!("Hashrate: {}", format_hashrate((count - last_count) as f64 / elapsed));
            last_count = count;
            last_time = Instant::now();
        }
    });
}

pub fn format_hashrate(hs: f64) -> String {
    match hs {
        h if h >= 1e9 => format!("{:.2} GH/s", h / 1e9),
        h if h >= 1e6 => format!("{:.2} MH/s", h / 1e6),
        h if h >= 1e3 => format!("{:.2} kH/s", h / 1e3),
        h => format!("{h:.1} H/s"),
    }
}

/// Hashes a synthetic job with an unreachable target and reports the rate.
async fn run_benchmark(slot: &WorkSlot, stats: &Stats, seconds: u64) {
    let job = pow::PowJob::new(&[0x5a; 32], 1_700_000_000_000, karlsen_math::Uint256::ZERO, karlsen_math::Uint256::ZERO);
    let generation = slot.next_generation();
    slot.publish(worker::WorkUnit::new(generation, job, worker::WorkOrigin::Stratum { job_id: "benchmark".into() }, 0, u64::MAX));
    // Skip the first seconds while threads warm up and fault in dataset pages.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let start_count = stats.hashes.load(Ordering::Relaxed);
    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let hashes = stats.hashes.load(Ordering::Relaxed) - start_count;
    info!("Benchmark: {} over {:.1}s", format_hashrate(hashes as f64 / start.elapsed().as_secs_f64()), start.elapsed().as_secs_f64());
}
