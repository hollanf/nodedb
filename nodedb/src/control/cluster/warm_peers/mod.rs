//! Pre-warm the QUIC peer cache after `TransportBind` so the
//! first replicated request after boot doesn't pay a cold
//! connect. Slots into the startup sequencer between
//! `TransportBind` and `WarmPeers` phases.

pub mod report;
pub mod warm;

pub use report::PeerWarmReport;
pub use warm::warm_known_peers;
