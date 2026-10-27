//! Stratum client for karlsen-stratum-bridge (and compatible pools).
//!
//! Protocol as implemented by karlsen-stratum-bridge for standard miners:
//!   → mining.subscribe   ["karlsen-cpu-miner/<version>"]
//!   → mining.authorize   ["<address>.<worker>", ""]
//!   ← mining.set_extranonce ["<hex>"] or ["<hex>", <extranonce2 size>]
//!   ← mining.set_difficulty [<f64>]
//!   ← mining.notify      [<job id>, [u64; 4] pre-PoW hash words (LE), <timestamp ms>]
//!   → mining.submit      ["<address>.<worker>", <job id>, "0x<16 hex nonce>"]
//!
//! In this mode the bridge builds and submits the block, so mass propagation is the
//! bridge's job; karlsen-stratum-bridge keeps it by converting templates through
//! consensus `Block` values, which carry each transaction's mass.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use karlsen_math::Uint256;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::pow::{share_target_from_difficulty, PowJob};
use crate::worker::{Solution, WorkOrigin, WorkSlot, WorkUnit};

pub struct StratumConfig {
    pub url: String,
    pub mining_address: String,
    pub worker_name: String,
    /// Require a pool certificate signed by a public CA. Off by default: most mining pools
    /// use self-signed certificates, and other miners (lolMiner, SRBMiner, …) accept them.
    pub strict_tls: bool,
}

/// Accepts any server certificate but still checks the handshake signatures, so the
/// connection is encrypted. Used for pools with self-signed certificates.
#[derive(Debug)]
struct AcceptPoolCertificate(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptPoolCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

struct Session {
    extranonce_prefix: u64,
    extranonce_free_mask: u64,
    difficulty: f64,
}

impl Default for Session {
    fn default() -> Self {
        Self { extranonce_prefix: 0, extranonce_free_mask: u64::MAX, difficulty: 1.0 }
    }
}

/// Splits a stratum URL into (host:port, use TLS).
fn parse_url(url: &str) -> (String, bool) {
    for (scheme, tls) in [("stratum+ssl://", true), ("stratum+tls://", true), ("ssl://", true), ("stratum+tcp://", false), ("stratum://", false), ("tcp://", false)] {
        if let Some(rest) = url.strip_prefix(scheme) {
            return (rest.to_string(), tls);
        }
    }
    (url.to_string(), false)
}

trait StratumStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> StratumStream for T {}

async fn open_stream(address: &str, tls: bool, strict_tls: bool) -> Result<Box<dyn StratumStream>> {
    let tcp = TcpStream::connect(address).await.with_context(|| format!("connecting to {address}"))?;
    tcp.set_nodelay(true)?;
    if !tls {
        return Ok(Box::new(tcp));
    }
    let host = address.rsplit_once(':').map(|(h, _)| h).unwrap_or(address).to_string();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone()).with_safe_default_protocol_versions()?;
    let config = if strict_tls {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(AcceptPoolCertificate(provider))).with_no_client_auth()
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(host.clone()).map_err(|_| anyhow!("invalid TLS host name {host}"))?;
    let stream = connector.connect(server_name, tcp).await.with_context(|| format!("TLS handshake with {host}"))?;
    Ok(Box::new(stream))
}

fn parse_extranonce(hex: &str) -> Result<(u64, u64)> {
    let hex = hex.trim_start_matches("0x");
    if hex.is_empty() {
        return Ok((0, u64::MAX));
    }
    if hex.len() > 8 || hex.len() % 2 != 0 {
        bail!("unsupported extranonce '{hex}'");
    }
    let value = u64::from_str_radix(hex, 16)?;
    let free_bits = 64 - 4 * hex.len() as u32;
    Ok((value << free_bits, (1u64 << free_bits) - 1))
}

fn parse_pre_pow(value: &Value) -> Result<[u8; 32]> {
    let words = value.as_array().ok_or_else(|| anyhow!("mining.notify header is not an array of 4 u64 words"))?;
    if words.len() != 4 {
        bail!("mining.notify header has {} words, expected 4", words.len());
    }
    let mut out = [0u8; 32];
    for (i, w) in words.iter().enumerate() {
        let w = w.as_u64().ok_or_else(|| anyhow!("mining.notify header word is not a u64"))?;
        out[i * 8..i * 8 + 8].copy_from_slice(&w.to_le_bytes());
    }
    Ok(out)
}

pub async fn run(
    config: StratumConfig,
    slot: Arc<WorkSlot>,
    mut solutions: UnboundedReceiver<Solution>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);
    while !shutdown.load(Ordering::Relaxed) {
        match session(&config, &slot, &mut solutions, &shutdown).await {
            Ok(()) => backoff = Duration::from_secs(1),
            Err(err) => {
                slot.pause();
                error!("Stratum session ended: {err:#}. Reconnecting in {}s", backoff.as_secs());
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
    Ok(())
}

async fn session(
    config: &StratumConfig,
    slot: &WorkSlot,
    solutions: &mut UnboundedReceiver<Solution>,
    shutdown: &AtomicBool,
) -> Result<()> {
    let (address, tls) = parse_url(&config.url);
    let stream = open_stream(&address, tls, config.strict_tls).await?;
    info!("Connected to stratum {address}{}", if tls { " (TLS)" } else { "" });
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut lines = BufReader::new(read_half).lines();

    let identity = format!("{}.{}", config.mining_address, config.worker_name);
    let agent = format!("karlsen-cpu-miner/{}", env!("CARGO_PKG_VERSION"));
    let mut next_id = 1u64;
    let mut send = |method: &str, params: Value| {
        let id = next_id;
        next_id += 1;
        (id, format!("{}\n", json!({ "id": id, "jsonrpc": "2.0", "method": method, "params": params })))
    };

    let (_, msg) = send("mining.subscribe", json!([agent]));
    write_half.write_all(msg.as_bytes()).await?;
    let (_, msg) = send("mining.authorize", json!([identity, ""]));
    write_half.write_all(msg.as_bytes()).await?;

    let mut state = Session::default();
    let (mut accepted, mut rejected) = (0u64, 0u64);
    let mut pending_submits = std::collections::HashMap::<u64, u64>::new();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        tokio::select! {
            line = lines.next_line() => {
                let line = line?.ok_or_else(|| anyhow!("connection closed by server"))?;
                if line.trim().is_empty() { continue; }
                let message: Value = serde_json::from_str(&line).with_context(|| format!("invalid JSON from server: {line}"))?;
                debug!("stratum ← {line}");

                if let Some(method) = message.get("method").and_then(Value::as_str) {
                    let params = message.get("params").cloned().unwrap_or(Value::Null);
                    match method {
                        "mining.set_extranonce" | "set_extranonce" => {
                            let hex = params.get(0).and_then(Value::as_str).unwrap_or("");
                            let (prefix, mask) = parse_extranonce(hex)?;
                            state.extranonce_prefix = prefix;
                            state.extranonce_free_mask = mask;
                            info!("Extranonce set to '{hex}'");
                        }
                        "mining.set_difficulty" | "set_difficulty" => {
                            if let Some(d) = params.get(0).and_then(Value::as_f64) {
                                state.difficulty = d;
                                info!("Share difficulty set to {d}");
                            }
                        }
                        "mining.notify" | "notify" => {
                            let job_id = params.get(0).cloned().ok_or_else(|| anyhow!("mining.notify without job id"))?;
                            let pre_pow = parse_pre_pow(params.get(1).unwrap_or(&Value::Null))?;
                            let timestamp = params.get(2).and_then(Value::as_u64).ok_or_else(|| anyhow!("mining.notify without timestamp"))?;
                            let share_target = share_target_from_difficulty(state.difficulty);
                            // The bridge does not send block bits; it decides itself whether a share is a block.
                            let pow = PowJob::new(&pre_pow, timestamp, Uint256::ZERO, share_target);
                            let generation = slot.next_generation();
                            slot.publish(WorkUnit::new(
                                generation,
                                pow,
                                WorkOrigin::Stratum { job_id: job_id.clone() },
                                state.extranonce_prefix,
                                state.extranonce_free_mask,
                            ));
                            debug!("New job {job_id} (generation {generation})");
                        }
                        other => debug!("ignoring stratum method {other}"),
                    }
                } else if let Some(id) = message.get("id").and_then(Value::as_u64) {
                    let ok = message.get("result").map(|r| r.as_bool().unwrap_or(!r.is_null())).unwrap_or(false)
                        && message.get("error").is_none_or(Value::is_null);
                    if let Some(nonce) = pending_submits.remove(&id) {
                        if ok {
                            accepted += 1;
                            info!("Share accepted (nonce {nonce:#018x}) · accepted {accepted}, rejected {rejected}");
                        } else {
                            rejected += 1;
                            warn!("Share rejected (nonce {nonce:#018x}): {} · accepted {accepted}, rejected {rejected}",
                                message.get("error").unwrap_or(&Value::Null));
                        }
                    } else if id == 2 {
                        if ok { info!("Authorized as {identity}"); } else { bail!("authorization failed: {line}"); }
                    }
                }
            }
            solution = solutions.recv() => {
                let Some(solution) = solution else { bail!("mining threads stopped") };
                let WorkOrigin::Stratum { job_id } = &solution.unit.origin else { continue };
                let (id, msg) = send("mining.submit", json!([identity, job_id, format!("0x{:016x}", solution.nonce)]));
                pending_submits.insert(id, solution.nonce);
                write_half.write_all(msg.as_bytes()).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_schemes() {
        assert_eq!(parse_url("stratum+ssl://pool.woolypooly.com:3132"), ("pool.woolypooly.com:3132".into(), true));
        assert_eq!(parse_url("stratum+tcp://127.0.0.1:5555"), ("127.0.0.1:5555".into(), false));
        assert_eq!(parse_url("127.0.0.1:5555"), ("127.0.0.1:5555".into(), false));
    }

    #[test]
    fn extranonce_two_bytes() {
        let (prefix, mask) = parse_extranonce("abcd").unwrap();
        assert_eq!(prefix, 0xabcd_0000_0000_0000);
        assert_eq!(mask, 0x0000_ffff_ffff_ffff);
    }

    #[test]
    fn pre_pow_words_are_little_endian() {
        let bytes = parse_pre_pow(&json!([1u64, 0u64, 0u64, 0x0100000000000000u64])).unwrap();
        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[31], 1);
    }
}
