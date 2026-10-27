# karlsen-cpu-miner

CPU miner for [Karlsen](https://karlsencoin.org) (KarlsenHashV2 / FishHashPlus), written in Rust.

Versioning continues from [karlsen-network/karlsen-miner-cpu](https://github.com/karlsen-network/karlsen-miner-cpu), whose last release was v0.2.0 (Leon1777, July 2025). This is a new Rust implementation; v0.3.0 is its first release.

GPUs are far faster, but CPUs are everywhere. When a large pool goes down and the network loses most of its hashrate, CPU miners on servers, desktops and (later) phones keep blocks coming. This miner is built for that role.

## Features

- **Setup wizard.** Run the miner without arguments: enter your wallet address, pick a pool, solo mining through a karlsen-stratum-bridge, or solo mining directly against karlsend, then threads and dataset storage. Pressing Enter at every step mines to the Karlsen treasury address on WoolyPooly with all CPU threads and a dataset file. Answers are saved to `karlsen-cpu-miner.json` next to the executable; `--wizard` runs it again.
- **Connection modes**
  - Pools over `stratum+tcp://` or TLS `stratum+ssl://` (built-in list below, any other pool via custom URL)
  - Solo through [karlsen-stratum-bridge](https://github.com/karlsencoin/karlsen-stratum-bridge) (`stratum+tcp://127.0.0.1:5555`)
  - Solo directly to `karlsend` over gRPC (`grpc://127.0.0.1:42110`)
- **Mass-preserving block submission (Crescendo-ready).** In solo mode the block template from `GetBlockTemplate` is submitted back exactly as received, with only the header nonce changed, so every transaction keeps the `mass` value the node computed. The miner checks this before every submission and refuses to send a block whose masses differ from the template. This fixes the node warning *"The RPC submitted block … contains a transaction … with mass = 0 … prior to the Crescendo hardfork activation"* that older miners trigger.
- **Bit-exact with the node.** The PoW is a re-implementation of `karlsen_pow::State` from rusty-karlsen v3.3.1 and is tested against it on every build.
- **Latency-hiding hash kernel.** Eight nonces are hashed in lock-step per thread and their 128-byte dataset reads are prefetched together, so each core keeps many memory requests in flight.
- **Three dataset modes**
  - `memory` (default): the 4.5 GiB dataset is built in RAM at start-up, with transparent huge pages on Linux
  - `file`: built once and saved to a file; later starts read the file into RAM (seconds on an SSD) instead of rebuilding it
  - `light`: no dataset (≈72 MiB light cache only); very slow, for testing and tiny devices
- Thread pinning, per-thread nonce chunks, a CPU re-check of every block candidate before submission, automatic stratum reconnect.

## Requirements

| | Minimum |
|---|---|
| Free RAM | **about 5 GiB** for `memory` / `file` modes (4,831,835,776-byte dataset plus 72 MiB light cache) |
| CPU | any x86-64 or ARM64 CPU; more cores and more memory channels mean more hashrate |
| Node (solo mode) | karlsend v3.3.x with `--utxoindex` and gRPC reachable |

FishHashPlus is memory-bound: hashrate depends mostly on memory latency and on how many requests the CPU can keep in flight, not on clock speed or AVX width.

## Build

```bash
# protoc is needed by the karlsend gRPC client crates
sudo apt-get install -y protobuf-compiler
cargo build --release
```

The binary is `target/release/karlsen-cpu-miner`.

## Built-in pools

| Pool | Payout | Stratum |
|---|---|---|
| WoolyPooly | PPLNS, 0.9% | `stratum+ssl://pool.woolypooly.com:3132` |
| TazMining (Switzerland) | PPLNS, 0.75% | `stratum+tcp://pool.tazmining.ch:7260` |
| Cedric-Crispin | PPLNS, 0.1% | `stratum+ssl://karlsencoin.cedric-crispin.com:4155` (SSL) · `stratum+tcp://karlsencoin.cedric-crispin.com:4154` |
| NushyPool (Europe) | PPS, 1.0% | `stratum+tcp://nushypool.com:40007` |

## Downloads

Prebuilt binaries for Windows, Linux (glibc 2.35+, Ubuntu 22.04 / HiveOS) and macOS are attached to every [release](https://github.com/karlsencoin/karlsen-cpu-miner/releases), with `SHA256SUMS`.

## Usage

First run (wizard):

```bash
./karlsen-cpu-miner
```

Command-line flags override the saved settings, and a full set of flags skips the wizard entirely.

Solo mining against a local node:

```bash
karlsen-cpu-miner \
  --mining-address karlsen:qzecljlvsd8h3x32ynuttzyya6s26p4p5p0gyztk7p984r25jp0y2s5fpdqhc \
  --node grpc://127.0.0.1:42110
```

Through a local karlsen-stratum-bridge:

```bash
karlsen-cpu-miner \
  --mining-address karlsen:qzecljlvsd8h3x32ynuttzyya6s26p4p5p0gyztk7p984r25jp0y2s5fpdqhc \
  --stratum stratum+tcp://127.0.0.1:5555 \
  --worker-name vps
```

Keep the dataset in a file so restarts are instant:

```bash
karlsen-cpu-miner ... --dataset-mode file --dataset-file /var/lib/karlsen-cpu-miner/fishhash-plus-dataset.bin
```

Benchmark without a node or pool:

```bash
karlsen-cpu-miner --mining-address karlsen:... --benchmark 60
```

### Options

| Option | Default | Meaning |
|---|---|---|
| `--wizard` | off | Run the setup wizard again |
| `-a, --mining-address` | — | Address that receives the rewards |
| `--node` | — | karlsend gRPC endpoint (solo mode) |
| `--stratum` | — | Stratum endpoint (bridge / pool mode), `stratum+tcp://` or `stratum+ssl://` |
| `--insecure-tls` | off | Accept any pool TLS certificate (only for pools with self-signed certificates). By default the operating system's certificate store verifies the pool, as KarlsenMiner does |
| `--worker-name` | `karlsen-cpu-miner` | Worker name sent to the stratum server |
| `-t, --threads` | all logical CPUs | Mining threads |
| `--no-pin` | off | Do not pin threads to cores |
| `--lanes` | measured at start | Nonces hashed together per thread: 4, 8 or 16. By default each is benchmarked for 2 s at start-up and the fastest is used |
| `--no-large-pages` | off | Keep the dataset on normal 4 KiB pages |
| `--dataset-mode` | `file` (wizard default) | `memory`, `file` or `light` |
| `--dataset-file` | `~/.cache/karlsen-cpu-miner/fishhash-plus-dataset.bin` | File for `file` mode |
| `--build-threads` | all logical CPUs | Threads used to build the dataset |
| `--poll-interval-ms` | `250` | Solo mode template refresh interval |
| `--mine-when-not-synced` | off | Keep mining while the node is syncing (testnets only) |
| `--benchmark <seconds>` | — | Measure hashrate and exit |

Log verbosity follows `RUST_LOG` (for example `RUST_LOG=debug`).

## Large pages

With 4 KiB pages almost every dataset read also misses the TLB. 2 MiB pages remove most of that cost.

- **Windows:** the miner uses large pages when the user has the *Lock pages in memory* right (the same setting XMRig uses). Run `secpol.msc` → Local Policies → User Rights Assignment → **Lock pages in memory** → add your user, then sign out and back in. The log says `Dataset uses 2 MiB large pages` when it is active. On a long-running PC, free RAM can be too fragmented for 4.5 GiB of large pages; a reboot fixes that.
- **Linux:** transparent huge pages are requested automatically (`/sys/kernel/mm/transparent_hugepage/enabled` must be `always` or `madvise`).

## How mass propagation works

`GetBlockTemplate` returns each transaction with a `mass` field filled in by the node. A miner must hand the same block back to `SubmitBlock` with those values intact. Miners that convert the template through an older wire format without a `mass` field submit `mass = 0`; the node accepts that today with a warning and will reject it after the Crescendo hardfork.

This miner keeps the template as an `RpcRawBlock` from `karlsen-rpc-core` and only writes `header.nonce` before submitting it. `MassAudit` in `src/node_client.rs`:

1. warns if the node itself ever sends a non-coinbase transaction with `mass = 0`;
2. compares the mass list of the block about to be submitted with the template and refuses to submit on any mismatch;
3. logs the total propagated mass with every accepted block.

In stratum mode the bridge builds and submits blocks; karlsen-stratum-bridge keeps mass because it converts templates through consensus `Block` values, which carry each transaction's mass.

## Tests

```bash
cargo test --release
```

`pow::node_equivalence` builds the light cache on both sides and checks that this miner's hashes and dataset items are identical to rusty-karlsen's.

## Community

- Discord: https://discord.gg/QyrvshRBJV
- Telegram: https://t.me/KarlsenCoin

## License

ISC
