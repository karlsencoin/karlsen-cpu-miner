//! Solo mining directly against a karlsend node over gRPC.
//!
//! Mass propagation (the node warning "RPC submitted block … contains a transaction with
//! mass = 0 … prior to the Crescendo hardfork activation"):
//!
//! GetBlockTemplate returns every transaction with its `mass` already computed by the node.
//! Older miners rebuilt the block through a wire format that had no mass field, so the
//! block arrived at SubmitBlock with `mass = 0` on every transaction. Once the hardfork is
//! active such blocks are rejected.
//!
//! This client never rebuilds transactions. It keeps the `RpcRawBlock` exactly as the node
//! sent it, changes only `header.nonce`, and submits that same object back. Every field,
//! `mass` included, therefore round-trips unchanged. `MassAudit` checks this on every
//! template and every submission and logs loudly if it is ever violated.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use karlsen_addresses::Address;
use karlsen_consensus_core::constants::BLOCK_VERSION_KHASHV2;
use karlsen_consensus_core::hashing::header::hash_override_nonce_time;
use karlsen_consensus_core::header::Header;
use karlsen_grpc_client::GrpcClient;
use karlsen_math::Uint256;
use karlsen_rpc_core::api::rpc::RpcApi;
use karlsen_rpc_core::{RpcRawBlock, SubmitBlockReport};
use log::{debug, error, info, warn};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::dataset::Dataset;
use crate::pow::PowJob;
use crate::worker::{Solution, WorkOrigin, WorkSlot, WorkUnit};

pub struct NodeConfig {
    pub url: String,
    pub mining_address: Address,
    pub extra_data: Vec<u8>,
    pub poll_interval: Duration,
    /// Mine even when the node reports it is not synced (testnets / isolated nodes only).
    pub mine_when_not_synced: bool,
}

/// Verifies that transaction mass values survive the template → submit round trip.
#[derive(Default)]
struct MassAudit {
    warned_zero_mass_in_template: bool,
}

impl MassAudit {
    /// Non-coinbase transactions are the ones the node assigns mass to; index 0 is the coinbase.
    fn masses(block: &RpcRawBlock) -> Vec<u64> {
        block.transactions.iter().map(|tx| tx.mass).collect()
    }

    fn check_template(&mut self, block: &RpcRawBlock) {
        let zero_mass = block
            .transactions
            .iter()
            .skip(1)
            .filter(|tx| tx.mass == 0)
            .count();
        if zero_mass > 0 && !self.warned_zero_mass_in_template {
            warn!(
                "GetBlockTemplate returned {zero_mass} non-coinbase transaction(s) with mass = 0. \
                 The miner forwards mass unchanged, so this comes from the node itself; check the karlsend version."
            );
            self.warned_zero_mass_in_template = true;
        }
    }

    /// Hard guarantee before SubmitBlock: same transaction count, same mass, in the same order.
    fn check_submission(template: &RpcRawBlock, submitted: &RpcRawBlock) -> Result<()> {
        let expected = Self::masses(template);
        let actual = Self::masses(submitted);
        if expected != actual {
            bail!(
                "mass mismatch between template and submission (template {:?}, submission {:?})",
                expected,
                actual
            );
        }
        Ok(())
    }
}

pub async fn connect(url: &str) -> Result<GrpcClient> {
    let url = if url.starts_with("grpc://") {
        url.to_string()
    } else {
        format!("grpc://{url}")
    };
    GrpcClient::connect(url.clone())
        .await
        .map_err(|e| anyhow!("connecting to karlsend at {url}: {e}"))
}

pub async fn run(
    config: NodeConfig,
    dataset: Arc<Dataset>,
    slot: Arc<WorkSlot>,
    mut solutions: UnboundedReceiver<Solution>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let client = Arc::new(connect(&config.url).await?);
    info!("Connected to karlsend at {}", config.url);
    let info = client.get_info().await.context("GetInfo")?;
    info!(
        "Node version {} · synced: {}",
        info.server_version, info.is_synced
    );

    // Submission task: verifies each candidate on the CPU once more and submits it untouched.
    let submit_client = client.clone();
    let submit_dataset = dataset.clone();
    let submit_slot = slot.clone();
    let submitter = tokio::spawn(async move {
        let mut accepted = 0u64;
        let mut rejected = 0u64;
        while let Some(solution) = solutions.recv().await {
            if !solution.meets_block_target {
                continue;
            }
            let WorkOrigin::Node { template } = &solution.unit.origin else {
                continue;
            };

            let verified = solution
                .unit
                .pow
                .hash_one(solution.nonce, &submit_dataset.source());
            if verified != solution.pow_value || verified > solution.unit.pow.block_target {
                error!(
                    "Discarding nonce {:#018x}: CPU re-verification failed",
                    solution.nonce
                );
                continue;
            }

            let mut block = template.clone();
            block.header.nonce = solution.nonce;
            if let Err(err) = MassAudit::check_submission(template, &block) {
                error!("Refusing to submit: {err}");
                continue;
            }
            let block_hash =
                karlsen_consensus_core::hashing::header::hash(&Header::from(&block.header));
            let tx_count = block.transactions.len();
            let total_mass: u64 = block.transactions.iter().map(|tx| tx.mass).sum();

            match submit_client.submit_block(block, false).await {
                Ok(response) => match response.report {
                    SubmitBlockReport::Success => {
                        accepted += 1;
                        info!(
                            "Block accepted: {block_hash} · {tx_count} tx · total mass {total_mass} propagated · accepted {accepted}, rejected {rejected}"
                        );
                        // The template is now stale; make every thread drop it immediately.
                        submit_slot.pause();
                    }
                    SubmitBlockReport::Reject(reason) => {
                        rejected += 1;
                        warn!("Block rejected: {block_hash} · reason: {reason:?} · accepted {accepted}, rejected {rejected}");
                    }
                },
                Err(err) => {
                    rejected += 1;
                    error!("SubmitBlock failed for {block_hash}: {err}");
                }
            }
        }
    });

    let mut audit = MassAudit::default();
    let mut last_pre_pow: Option<[u8; 32]> = None;
    let mut last_published = Instant::now();
    let mut warned_not_synced = false;

    while !shutdown.load(Ordering::Relaxed) {
        match client
            .get_block_template(config.mining_address.clone(), config.extra_data.clone())
            .await
        {
            Ok(response) => {
                if !response.is_synced && !config.mine_when_not_synced {
                    if !warned_not_synced {
                        warn!("Node reports it is not synced; mining paused until it is");
                        warned_not_synced = true;
                    }
                    slot.pause();
                    last_pre_pow = None;
                } else {
                    warned_not_synced = false;
                    let template = response.block;
                    if template.header.version != BLOCK_VERSION_KHASHV2 {
                        bail!("unsupported block version {} (this miner implements KarlsenHashV2 only)", template.header.version);
                    }
                    audit.check_template(&template);

                    let header = Header::from(&template.header);
                    let pre_pow = hash_override_nonce_time(&header, 0, 0).as_bytes();
                    let refresh_timestamp = last_published.elapsed() > Duration::from_secs(2);
                    if last_pre_pow != Some(pre_pow) || refresh_timestamp {
                        let target = Uint256::from_compact_target_bits(header.bits);
                        let pow = PowJob::new(&pre_pow, header.timestamp, target, target);
                        let generation = slot.next_generation();
                        debug!(
                            "New template: DAA score {} · {} tx · bits {:#010x} · generation {generation}",
                            header.daa_score,
                            template.transactions.len(),
                            header.bits
                        );
                        slot.publish(WorkUnit::new(
                            generation,
                            pow,
                            WorkOrigin::Node { template },
                            0,
                            u64::MAX,
                        ));
                        last_pre_pow = Some(pre_pow);
                        last_published = Instant::now();
                    }
                }
            }
            Err(err) => {
                error!("GetBlockTemplate failed: {err}");
                slot.pause();
                last_pre_pow = None;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        tokio::time::sleep(config.poll_interval).await;
    }

    submitter.abort();
    let _ = client.disconnect().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use karlsen_rpc_core::{RpcRawHeader, RpcSubnetworkId, RpcTransaction};

    fn tx(mass: u64) -> RpcTransaction {
        RpcTransaction {
            version: 0,
            inputs: vec![],
            outputs: vec![],
            lock_time: 0,
            subnetwork_id: RpcSubnetworkId::from_byte(0),
            gas: 0,
            payload: vec![],
            mass,
            verbose_data: None,
        }
    }

    fn block(masses: &[u64]) -> RpcRawBlock {
        RpcRawBlock {
            header: RpcRawHeader {
                version: BLOCK_VERSION_KHASHV2,
                parents_by_level: vec![],
                hash_merkle_root: Default::default(),
                accepted_id_merkle_root: Default::default(),
                utxo_commitment: Default::default(),
                timestamp: 0,
                bits: 0,
                nonce: 0,
                daa_score: 0,
                blue_work: Default::default(),
                blue_score: 0,
                pruning_point: Default::default(),
            },
            transactions: masses.iter().map(|&m| tx(m)).collect(),
        }
    }

    #[test]
    fn nonce_change_keeps_mass() {
        let template = block(&[0, 2036, 1250]);
        let mut submitted = template.clone();
        submitted.header.nonce = 0xdead_beef;
        assert!(MassAudit::check_submission(&template, &submitted).is_ok());
    }

    #[test]
    fn zeroed_mass_is_refused() {
        let template = block(&[0, 2036, 1250]);
        let submitted = block(&[0, 0, 0]);
        assert!(MassAudit::check_submission(&template, &submitted).is_err());
    }
}
