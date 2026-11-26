mod affinity;
mod config;
mod meter;
mod pool;
pub mod proxy;
mod stats;
mod usage;

pub use config::{KeyEntry, Keys, ParseError};
pub use meter::{Meter, key_fingerprint};
pub use pool::{AccountInfo, Pool};
pub use proxy::Server;
pub use stats::{Range as StatsRange, STATS_TTL, StatsSnapshot, StatsTracker};
pub use usage::{KeyUsage, USAGE_TTL, UsageSnapshot, UsageTracker};

/// Version from Cargo.toml, for --version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
