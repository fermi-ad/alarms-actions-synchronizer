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

        // Need to nest this one more level due to the `v1` suffix. Otherwise the protobuf Timestamp reference won't line up.
        pub use commands::*;
        mod commands {
            tonic::include_proto!("services.alarm_commands.v1");
        }
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
    AlarmStateCache, CachedState, ControlsObservedStatePolicy, PhoebusObservedStatePolicy,
    read_controls_observed_state_policy, read_phoebus_observed_state_policy,
    record_config_hydrated_state, record_controls_observed_state, record_phoebus_observed_state,
    record_state_hydrated_state,
};

pub use config::{
    ConfigLoadError, LoggingInitError, RuntimeSyncFactory, Synchronizer, SynchronizerConfig,
};

pub use outcomes::{
    IgnoreReason, OutOfScopeReason, OutboundSyncResult, SkipReason, SyncDirection, SyncOutcome,
};
