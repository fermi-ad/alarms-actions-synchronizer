//! Models Module
//!
//! Provides the common data structures used throughout the application.

pub mod cache;
pub mod config;
pub mod metadata;
pub mod outcomes;
pub mod phoebus;
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

#[cfg(test)]
mod tests;

/// The command that will come in from/should be sent to Phoebus during an acknowledgement.
pub const ACK_COMMAND: &str = "acknowledge";

pub use cache::{
    AlarmStateCache, CachedState, ObservedStatePolicy, read_observed_state_policy,
    record_alarm_state, record_startup_state_evidence,
};

pub use config::{
    ConfigLoadError, LoggingInitError, RuntimeSyncFactory, Synchronizer, SynchronizerConfig,
};

pub use outcomes::{IgnoreReason, OutboundSyncResult, SkipReason, SyncDirection, SyncOutcome};
