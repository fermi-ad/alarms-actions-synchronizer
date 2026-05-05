//! Models Module
//!
//! Provides the common data structures used throughout the application.

pub mod cache;
pub mod config;
pub mod metadata;
pub mod outcomes;
pub mod phoebus;

mod common {
    pub mod alarm {
        //! Alarm Module
        //!
        //! Contains the auto-generated alarms structs from Protobuf,
        //! for use when de/serializing records from the Controls Kafka instance.
        //! Also contains the gRPC interface for issuing commands to the Controls
        //! alarm service.
        tonic::include_proto!("common.alarm");
        tonic::include_proto!("services.alarm_commands");
    }
}

mod google {
    pub mod protobuf {
        //! Generated Google Structs Module
        //!
        //! Contains the builtin structures (mainly [`Timestamp`]) provided by Google
        tonic::include_proto!("google.protobuf");
    }
}

#[cfg(test)]
mod tests;

/// The command that will come in from/should be sent to Phoebus during an acknowledgement.
pub const ACK_COMMAND: &str = "acknowledge";

pub use common::alarm;
pub use google::protobuf as generated;

pub use cache::{
    AlarmStateCache, CachedState, ObservedStatePolicy, read_observed_state_policy,
    record_alarm_state, record_startup_state_evidence,
};

pub use config::{
    ConfigLoadError, LoggingInitError, RuntimeSyncFactory, Synchronizer, SynchronizerConfig,
};

pub use outcomes::{IgnoreReason, OutboundSyncResult, SkipReason, SyncDirection, SyncOutcome};
