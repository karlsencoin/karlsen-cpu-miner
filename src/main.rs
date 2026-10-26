//! karlsen-cpu-miner — CPU miner for Karlsen (KarlsenHashV2 / FishHashPlus).
//!
//! Run without arguments for the setup wizard (wallet, pool / solo bridge / solo node,
//! threads, dataset storage); the answers are saved to `karlsen-cpu-miner.json` and reused.
//! Every setting can also be given on the command line:
//! * `--node grpc://host:42110`          solo mining against karlsend, mass-preserving submission
//! * `--stratum stratum+tcp://host:port` mining through karlsen-stratum-bridge or a pool

mod config;
mod dataset;
mod fishhash;
mod node_client;
mod pools;
mod pow;
mod stratum_client;
mod wizard;
mod worker;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use clap::Parser;
use karlsen_addresses::Address;
use log::info;

use config::{Connection, MinerConfig};
use dataset::{default_dataset_path, Dataset, DatasetMode};
use worker::{spawn_workers, Stats, WorkSlot, WorkerConfig};

#[derive(Parser, Debug)]
#[command(name = "karlsen-cpu-miner", version, about = "CPU miner for Karlsen (KarlsenHashV2 / FishHashPlus)")]
struct Args {
    /// Run the setup wizard even if a saved configuration exists.
    #[arg(long)]
    wizard: bool,

    /// Karlsen address that receives the rewards (karlsen:…).
    #[arg(long, short = 'a')]
    mining_address: Option<String>,

    /// Solo mining: karlsend gRPC endpoint, e.g. grpc://127.0.0.1:42110.
    #[arg(long, conflicts_with = "stratum")]
    node: Option<String>,

    /// Pool / bridge mining: stratum endpoint, e.g. stratum+tcp://127.0.0.1:5555 or stratum+ssl://host:port.
    #[arg(long, conflicts_with = "node")]
    stratum: Option<String>,

    /// Worker name sent to the stratum server.
    #[arg(long)]
    worker_name: Option<String>,

    /// Mining threads (default: all logical CPUs).
    #[arg(long, short = 't')]
    threads: Option<usize>,

    /// Do not pin mining threads to CPU cores.
    #[arg(long)]
    no_pin: bool,

    /// Where the 4.5 GiB FishHashPlus dataset lives.
    #[arg(long, value_enum)]
    dataset_mode: Option<DatasetMode>,

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

/// Builds the effective configuration: command line first, then the saved file, then the wizard.
fn resolve_config(args: &Args) -> Result<MinerConfig> {
    let cli_connection = match (&args.node, &args.stratum) {
        (Some(url), _) => Some(Connection::SoloNode { url: url.clone() }),
        (_, Some(url)) => Some(Connection::CustomStratum { url: url.clone() }),
        _ => None,
    };

    let saved = config::load()?;
    let complete_from_cli = args.mining_address.is_some() && (cli_connection.is_some() || args.benchmark.is_some());

    let mut config = if args.wizard || (saved.is_none() && !complete_from_cli) {
        let config = wizard::run(saved.as_ref())?;
        let path = config::save(&config)?;
        println!("  Saved to {}\n", path.display());
        config
    } else if let Some(saved) = saved.filter(|_| !complete_from_cli) {
        info!("Using saved configuration {} (run with --wizard to change it)", config::config_path().display());
        saved
    } else {
        MinerConfig {
            mining_address: String::new(),
            connection: Connection::CustomStratum { url: String::new() },
            worker_name: "karlsen-cpu-miner".into(),
            threads: None,
            dataset_mode: DatasetMode::File,
            dataset_file: None,
        }
    };

    // Explicit command-line values always win over saved ones.
    if let Some(address) = &args.mining_address {
        config.mining_address = address.clone();
    }
    if let Some(connection) = cli_connection {
        config.connection = connection;
    }
    if let Some(worker) = &args.worker_name {
        config.worker_name = worker.clone();
    }
    if args.threads.is_some() {
        config.threads = args.threads;
    }
    if let Some(mode) = args.dataset_mode {
        config.dataset_mode = mode;
    }
    if args.dataset_file.is_some() {
        config.dataset_file = args.dataset_file.clone();
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).format_timestamp_secs().init();
    let args = Args::parse();
    let config = resolve_config(&args)?;

    let mining_address = Address::try_from(config.mining_address.as_str())
        .map_err(|e| anyhow!("invalid mining address '{}': {e}", config.mining_address))?;

    let logical = num_cpus::get();
    let threads = config.threads.unwrap_or(logical).max(1);
    let build_threads = args.build_threads.unwrap_or(logical).max(1);
    info!("karlsen-cpu-miner {} · {} mining threads · dataset mode {:?}", env!("CARGO_PKG_VERSION"), threads, config.dataset_mode);
    if args.benchmark.is_none() {
        info!("Mining: {}", config.connection.describe());
    }

    let dataset_mode = config.dataset_mode;
    let dataset_path = config.dataset_file.clone().unwrap_or_else(default_dataset_path);
    let dataset = tokio::task::spawn_blocking(move || Dataset::load(dataset_mode, &dataset_path, build_threads)).await??;

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
    } else {
        match config.connection.clone() {
            Connection::SoloNode { url } => {
                let extra = format!("karlsen-cpu-miner/{}", env!("CARGO_PKG_VERSION")).into_bytes();
                node_client::run(
                    node_client::NodeConfig {
                        url,
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
            }
            Connection::Pool { url, .. } | Connection::SoloBridge { url } | Connection::CustomStratum { url } => {
                stratum_client::run(
                    stratum_client::StratumConfig {
                        url,
                        mining_address: config.mining_address.clone(),
                        worker_name: config.worker_name.clone(),
                    },
                    slot.clone(),
                    solution_rx,
                    shutdown.clone(),
                )
                .await
            }
        }
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
    slot.publish(worker::WorkUnit::new(generation, job, worker::WorkOrigin::Stratum { job_id: serde_json::Value::Null }, 0, u64::MAX));
    // Skip the first seconds while threads warm up and fault in dataset pages.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let start_count = stats.hashes.load(Ordering::Relaxed);
    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let hashes = stats.hashes.load(Ordering::Relaxed) - start_count;
    info!("Benchmark: {} over {:.1}s", format_hashrate(hashes as f64 / start.elapsed().as_secs_f64()), start.elapsed().as_secs_f64());
}
