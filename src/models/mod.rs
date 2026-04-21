//! Models Module
//!
//! Provides the common data structures used throughout the application.

pub use common::alarm;
pub use google::protobuf as generated;

use alarm::Status;
use alarm::status::State;
use chrono::{DateTime, TimeZone, Utc};
use generated::Timestamp;
use rust_pubsub_lib::{Publisher, Snapshot, Subscriber};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub mod phoebus;

#[cfg(test)]
mod tests;

/// The command that will come in from/should be sent to Phoebus during an acknowledgement.
pub const ACK_COMMAND: &str = "acknowledge";

/// Alias for the atomic cache of alarm state data, shared between the two synchronizing processes.
///
/// This cache is the synchronizer's local memory of the latest in-scope alarm-handling state it has
/// observed for each EPICS device. It is intentionally used for duplicate suppression and loop avoidance,
/// even when an outbound publish or RPC fails.
///
/// The values in this cache therefore mean "latest observed by the synchronizer" rather than
/// "latest confirmed mirrored to the opposite system".
pub type AlarmStateCache = Arc<RwLock<HashMap<String, CachedState>>>;

/// Alias for the atomic cache of PV metadata shared between the two synchronizing processes.
///
/// This cache defines which EPICS devices are currently in scope for synchronization. A device becomes
/// eligible for synchronization only after Phoebus emits configuration metadata for it, whether during
/// startup hydration or later runtime monitoring.
pub type PvCache = Arc<RwLock<HashMap<String, phoebus::PvMetadata>>>;

/// Structured summary of what happened while processing a synchronization-relevant event.
///
/// The variants are designed to distinguish duplicate suppression, out-of-scope filtering, skipped work,
/// attempted propagation, and startup hydration. Stage 1 uses this model to make synchronization semantics
/// explicit without yet changing the existing anti-loop cache-write policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The inbound value matched the synchronizer's latest observed state, so nothing was mirrored.
    Duplicate,

    /// The message was intentionally ignored because it carries no synchronization-relevant user intent.
    Ignored { reason: IgnoreReason },

    /// The device is currently out of scope because Phoebus configuration metadata is not known.
    OutOfScope { reason: OutOfScopeReason },

    /// A synchronization attempt was skipped even though the message was otherwise relevant.
    Skipped { reason: SkipReason },

    /// The synchronizer attempted propagation to the opposite system.
    Attempted {
        direction: SyncDirection,
        result: AttemptResult,
    },

    /// State or metadata was hydrated from startup evidence rather than runtime user intent.
    Hydrated { source: HydrationSource },
}

/// The direction in which synchronization was attempted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncDirection {
    ControlsToPhoebus,
    PhoebusToControls,
}

/// Why an inbound message was ignored without any synchronization attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IgnoreReason {
    /// The device belongs to a non-EPICS source and is outside this synchronizer's mission.
    NonEpicsSource,
    /// The message class is part of Phoebus traffic but does not express user intent this service mirrors.
    NonSyncOperation,
    /// A Phoebus message class was recognized as server-state or otherwise irrelevant noise.
    NonSyncPhoebusMessage,
}

/// Why a device was treated as out of scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutOfScopeReason {
    /// No Phoebus configuration metadata is known for this EPICS device yet.
    MissingPhoebusMetadata,
}

/// Why a synchronization-relevant action was skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// No topic could be resolved for the requested Phoebus operation.
    MissingTopic,
    /// No publisher/client capability existed for the resolved destination.
    MissingPublisher,
    /// An upstream API or feature is intentionally unavailable in this repository.
    UnsupportedCapability,
    /// Input was malformed for the expected message class.
    MalformedMessage,
}

/// Result of attempting synchronization work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptResult {
    Succeeded,
    Failed,
}

/// Provenance for cache entries created during startup hydration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HydrationSource {
    PhoebusConfig,
    PhoebusState,
}

/// Encapsulates the latest state information about an alarm.
#[derive(Clone, Debug, PartialEq)]
pub struct CachedState {
    /// The latest [`State`] the sync service has recorded for the alarm.
    ///
    /// This is the latest in-scope state the synchronizer has observed locally, not necessarily the latest
    /// state that has been confirmed as mirrored successfully to the opposite system.
    pub state: State,
    /// If the alarm is snoozed, this field will be set to [`Some`] with the reenablement time, and
    /// the [`state`](Self::state) field will be set to [`Bypassed`](State::Bypassed).
    /// Otherwise, this field will be [`None`].
    pub wake: Option<Timestamp>,
}
impl CachedState {
    pub fn bypassed() -> Self {
        Self {
            state: State::Bypassed,
            wake: None,
        }
    }
}
impl Default for CachedState {
    fn default() -> Self {
        Self {
            state: State::Unknown,
            wake: None,
        }
    }
}
impl From<Status> for CachedState {
    fn from(value: Status) -> Self {
        CachedState {
            state: value.state(),
            wake: value.wake,
        }
    }
}
impl From<bool> for CachedState {
    fn from(is_active: bool) -> Self {
        Self {
            state: if is_active {
                State::Ok
            } else {
                State::Bypassed
            },
            wake: None,
        }
    }
}
impl<Tz: TimeZone> From<DateTime<Tz>> for CachedState {
    fn from(value: DateTime<Tz>) -> Self {
        if value.timestamp_millis() > Utc::now().timestamp_millis() {
            Self {
                state: State::Bypassed,
                wake: Some(Timestamp {
                    seconds: value.timestamp(),
                    nanos: value.timestamp_subsec_nanos() as i32,
                }),
            }
        } else {
            Self {
                state: State::Ok,
                wake: None,
            }
        }
    }
}

/// Configuration data to initialize the synchronizer processes.
#[derive(Debug)]
pub struct SynchronizerConfig {
    /// A reference to the shared cache of alarm state data.
    pub alarm_states: AlarmStateCache,

    /// A [`CancellationToken`] to handle gracefully shutting down the tokio runtime.
    pub cancel_token: CancellationToken,

    /// The location of the Controls Kafka instance.
    pub controls_host: String,

    /// The topic to read/write Controls messages from/to.
    pub controls_topic: String,

    /// The location of the gRPC alarms service for Controls.
    pub grpc_alarms_svc_host: String,

    /// The location of the Phoebus Kafka instance.
    pub phoebus_host: String,

    /// The [`Vec`] of topics to read/write from/to for Phoebus messages.
    ///
    /// This will just contain the base names of the topics. That is, Phoebus requires
    /// each "topic" be split into 3 parts: a vanilla topic to hold the state and config records,
    /// a "Command" topic for clients to send commands to the service, and a "Talk" topic for the service
    /// to send messages for annunciation.
    ///
    /// Each instance of [`Synchronizer`] will determine which, if any, of the auxiliary topics it will interact with.
    pub phoebus_topics: Vec<String>,

    /// A reference to the shared cache of PV metadata.
    pub pv_metadata: PvCache,
}
impl SynchronizerConfig {
    /// Creates a new instance of [`SynchronizerConfig`] from the provided hosts and topics.
    ///
    /// As part of the initialization, this constructor will generate the shared atomic caches
    /// on the heap.
    pub fn new(
        cancel_token: CancellationToken,
        controls_host: String,
        controls_topic: String,
        grpc_alarms_svc_host: String,
        phoebus_host: String,
        phoebus_topics: Vec<String>,
    ) -> Self {
        SynchronizerConfig {
            alarm_states: Arc::new(RwLock::new(HashMap::<String, CachedState>::new())),
            cancel_token,
            controls_host,
            controls_topic,
            grpc_alarms_svc_host,
            phoebus_host,
            phoebus_topics,
            pv_metadata: Arc::new(RwLock::new(HashMap::<String, phoebus::PvMetadata>::new())),
        }
    }
}
impl Clone for SynchronizerConfig {
    /// Generates a new instance with copies of the references to the shared caches.
    /// That is, this will NOT create new caches on the heap, but references the same
    /// instances as the [`SynchronizerConfig`] instance being cloned.
    fn clone(&self) -> Self {
        SynchronizerConfig {
            alarm_states: Arc::clone(&self.alarm_states),
            cancel_token: self.cancel_token.clone(),
            controls_host: self.controls_host.clone(),
            controls_topic: self.controls_topic.clone(),
            grpc_alarms_svc_host: self.grpc_alarms_svc_host.clone(),
            phoebus_host: self.phoebus_host.clone(),
            phoebus_topics: self.phoebus_topics.clone(),
            pv_metadata: Arc::clone(&self.pv_metadata),
        }
    }
}
impl PartialEq for SynchronizerConfig {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.alarm_states, &other.alarm_states)
            && self.controls_host == other.controls_host
            && self.controls_topic == other.controls_topic
            && self.grpc_alarms_svc_host == other.grpc_alarms_svc_host
            && self.phoebus_host == other.phoebus_host
            && self.phoebus_topics == other.phoebus_topics
            && Arc::ptr_eq(&self.pv_metadata, &other.pv_metadata)
    }
}

/// A trait to describe the basic functions of a synchronization process.
#[async_trait::async_trait]
pub trait Synchronizer<P: Publisher, S: Subscriber> {
    /// Constructs a [`Synchronizer`] instance from the provided [`SynchronizerConfig`] instance.
    fn new(config: SynchronizerConfig) -> Self;

    /// Kicks off the async process to monitor for alarm updates that need synchronization.
    async fn synchronize<SNAP: Snapshot>(self);
}

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
