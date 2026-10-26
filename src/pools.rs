//! Built-in Karlsen pool list shown by the setup wizard.
//!
//! Endpoints are taken from each pool's own connection page;
//! any other pool can still be used through "Custom stratum URL" in the wizard.

pub struct PoolEntry {
    /// Name shown in the wizard.
    pub display_name: &'static str,
    /// Payout scheme and fee as advertised by the pool.
    pub payout: &'static str,
    pub stratum_url: &'static str,
}

pub const POOLS: &[PoolEntry] = &[
    PoolEntry {
        display_name: "WoolyPooly (woolypooly.com)",
        payout: "PPLNS, 0.9% fee",
        stratum_url: "stratum+ssl://pool.woolypooly.com:3132",
    },
    PoolEntry {
        display_name: "TazMining (tazmining.ch, Switzerland)",
        payout: "PPLNS, 0.75% fee",
        stratum_url: "stratum+tcp://pool.tazmining.ch:7260",
    },
    PoolEntry {
        display_name: "Cedric-Crispin (karlsencoin.cedric-crispin.com) SSL",
        payout: "PPLNS, 0.1% fee",
        stratum_url: "stratum+ssl://karlsencoin.cedric-crispin.com:4155",
    },
    PoolEntry {
        display_name: "Cedric-Crispin (karlsencoin.cedric-crispin.com) no SSL",
        payout: "PPLNS, 0.1% fee",
        stratum_url: "stratum+tcp://karlsencoin.cedric-crispin.com:4154",
    },
    PoolEntry {
        display_name: "NushyPool (nushypool.com, Europe)",
        payout: "PPS, 1.0% fee, no payment fees",
        stratum_url: "stratum+tcp://nushypool.com:40007",
    },
];

/// Karlsen treasury address, offered as the wallet default so a first run needs no typing.
pub const TREASURY_ADDRESS: &str = "karlsen:qzecljlvsd8h3x32ynuttzyya6s26p4p5p0gyztk7p984r25jp0y2s5fpdqhc";

/// Default address of a karlsen-stratum-bridge running on the same machine.
pub const LOCAL_BRIDGE_URL: &str = "stratum+tcp://127.0.0.1:5555";
/// Default karlsend gRPC endpoint on the same machine.
pub const LOCAL_NODE_URL: &str = "grpc://127.0.0.1:42110";
