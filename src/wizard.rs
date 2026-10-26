//! Interactive first-run setup: wallet address, where to mine, threads, dataset storage.

use std::io::{self, BufRead, Write};

use anyhow::{bail, Result};
use karlsen_addresses::Address;

use crate::config::{Connection, MinerConfig};
use crate::dataset::{default_dataset_path, DatasetMode};
use crate::fishhash::FULL_DATASET_BYTES;
use crate::pools::{LOCAL_BRIDGE_URL, LOCAL_NODE_URL, POOLS};

fn read_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut line = String::new();
    if io::stdin().lock().read_line(&mut line)? == 0 {
        bail!("setup cancelled (end of input)");
    }
    Ok(line.trim().to_string())
}

fn prompt_default(label: &str, default: &str) -> Result<String> {
    let answer = read_line(&format!("  {label} [{default}]: "))?;
    Ok(if answer.is_empty() { default.to_string() } else { answer })
}

fn prompt_choice(label: &str, max: usize, default: usize) -> Result<usize> {
    loop {
        let answer = read_line(&format!("  {label} [{default}]: "))?;
        if answer.is_empty() {
            return Ok(default);
        }
        match answer.parse::<usize>() {
            Ok(n) if (1..=max).contains(&n) => return Ok(n),
            _ => println!("  Please enter a number between 1 and {max}."),
        }
    }
}

fn section(title: &str) {
    println!();
    println!("  -----------------------------------------------------");
    println!("  {title}");
    println!("  -----------------------------------------------------");
}

/// MemAvailable from /proc/meminfo on Linux; `None` elsewhere.
fn available_memory_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|l| l.starts_with("MemAvailable:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

fn gib(bytes: u64) -> String {
    format!("{:.2} GiB", bytes as f64 / (1u64 << 30) as f64)
}

pub fn run(previous: Option<&MinerConfig>) -> Result<MinerConfig> {
    println!();
    println!("  =====================================================");
    println!("  karlsen-cpu-miner {} setup", env!("CARGO_PKG_VERSION"));
    println!("  =====================================================");

    // Step 1: wallet address.
    section("Step 1/5 · Wallet address");
    println!("  Rewards are paid to this Karlsen address (starts with karlsen:).");
    let mining_address = loop {
        let default = previous.map(|c| c.mining_address.as_str()).unwrap_or("");
        let answer = if default.is_empty() { read_line("  Wallet address: ")? } else { prompt_default("Wallet address", default)? };
        match Address::try_from(answer.as_str()) {
            Ok(address) if address.prefix.to_string() == "karlsen" => break answer,
            Ok(_) => println!("  That is not a mainnet address (it must start with karlsen:)."),
            Err(err) => println!("  Invalid address: {err}"),
        }
    };

    // Step 2: where to mine.
    section("Step 2/5 · Where to mine");
    let mut option = 1;
    for pool in POOLS {
        println!("    {option}. {} · {} · {}", pool.display_name, pool.payout, pool.stratum_url);
        option += 1;
    }
    let solo_bridge = option;
    println!("    {solo_bridge}. Solo through karlsen-stratum-bridge (bridge next to your own karlsend node)");
    let solo_node = option + 1;
    println!("    {solo_node}. Solo directly to karlsend over gRPC (no bridge needed)");
    let custom = option + 2;
    println!("    {custom}. Custom stratum URL (any other pool)");
    let choice = prompt_choice("Select", custom, 1)?;
    let connection = if choice <= POOLS.len() {
        let pool = &POOLS[choice - 1];
        Connection::Pool { name: pool.display_name.to_string(), url: pool.stratum_url.to_string() }
    } else if choice == solo_bridge {
        Connection::SoloBridge { url: prompt_default("Bridge stratum URL", LOCAL_BRIDGE_URL)? }
    } else if choice == solo_node {
        println!("  The node needs --utxoindex. Blocks are submitted exactly as the node built them,");
        println!("  so transaction mass is preserved (required after the Crescendo hardfork).");
        Connection::SoloNode { url: prompt_default("karlsend gRPC URL", LOCAL_NODE_URL)? }
    } else {
        Connection::CustomStratum {
            url: loop {
                let url = read_line("  Stratum URL (stratum+tcp://host:port or stratum+ssl://host:port): ")?;
                if url.contains(':') {
                    break url;
                }
                println!("  Please include host and port.");
            },
        }
    };

    // Step 3: worker name.
    section("Step 3/5 · Worker name");
    let default_worker = previous.map(|c| c.worker_name.clone()).unwrap_or_else(|| "karlsen-cpu-miner".to_string());
    let worker_name = prompt_default("Worker name", &default_worker)?;

    // Step 4: threads.
    section("Step 4/5 · CPU threads");
    let logical = num_cpus::get();
    let physical = num_cpus::get_physical();
    println!("  Detected {physical} physical cores / {logical} logical CPUs.");
    println!("  FishHashPlus is memory-bound; leave a thread free if this machine runs other services.");
    let threads = loop {
        let answer = prompt_default("Mining threads", &logical.to_string())?;
        match answer.parse::<usize>() {
            Ok(n) if n >= 1 => break if n == logical { None } else { Some(n) },
            _ => println!("  Please enter a positive number."),
        }
    };

    // Step 5: dataset storage.
    section("Step 5/5 · FishHashPlus dataset (4.5 GiB)");
    let needed = FULL_DATASET_BYTES as u64 + (80 << 20);
    match available_memory_bytes() {
        Some(available) if available < needed => println!(
            "  WARNING: only {} of RAM is available, the dataset needs about {}. Mining will swap or fail.",
            gib(available),
            gib(needed)
        ),
        Some(available) => println!("  Available RAM: {} (dataset needs about {}).", gib(available), gib(needed)),
        None => println!("  The dataset needs about {} of free RAM.", gib(needed)),
    }
    println!("    1. File   · built once and saved to disk, instant restarts (recommended)");
    println!("    2. Memory · rebuilt in RAM on every start");
    println!("    3. Light  · no dataset, extremely slow (testing only)");
    let dataset_mode = match prompt_choice("Select", 3, 1)? {
        1 => DatasetMode::File,
        2 => DatasetMode::Memory,
        _ => DatasetMode::Light,
    };
    let dataset_file = if dataset_mode == DatasetMode::File {
        let default = default_dataset_path();
        let answer = prompt_default("Dataset file", &default.display().to_string())?;
        Some(answer.into())
    } else {
        None
    };

    let config = MinerConfig { mining_address, connection, worker_name, threads, dataset_mode, dataset_file };

    section("Summary");
    println!("  Wallet:  {}", config.mining_address);
    println!("  Mining:  {}", config.connection.describe());
    println!("  Worker:  {}", config.worker_name);
    println!("  Threads: {}", config.threads.map(|t| t.to_string()).unwrap_or_else(|| format!("{logical} (all)")));
    println!("  Dataset: {:?}", config.dataset_mode);
    let start = prompt_default("Save and start? (y/n)", "y")?;
    if !start.eq_ignore_ascii_case("y") && !start.eq_ignore_ascii_case("yes") {
        bail!("setup cancelled");
    }
    Ok(config)
}
