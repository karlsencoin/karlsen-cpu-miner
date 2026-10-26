//! Built-in Karlsen pool list shown by the setup wizard.
//!
//! Only endpoints that have been verified against the live pool are listed here;
//! any other pool can still be used through "Custom stratum URL" in the wizard.

pub struct PoolEntry {
    /// Name shown in the wizard.
    pub display_name: &'static str,
    /// Payout scheme and fee as advertised by the pool.
    pub payout: &'static str,
    pub stratum_url: &'static str,
}

pub const POOLS: &[PoolEntry] = &[PoolEntry {
    display_name: "WoolyPooly (woolypooly.com)",
    payout: "PPLNS, 0.9% fee",
    stratum_url: "stratum+ssl://pool.woolypooly.com:3132",
}];

/// Default address of a karlsen-stratum-bridge running on the same machine.
pub const LOCAL_BRIDGE_URL: &str = "stratum+tcp://127.0.0.1:5555";
/// Default karlsend gRPC endpoint on the same machine.
pub const LOCAL_NODE_URL: &str = "grpc://127.0.0.1:42110";
