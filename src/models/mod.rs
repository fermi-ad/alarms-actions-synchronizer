//! Models Module
//!
//! Provides the common data structures used throughout the application.

pub use common::alarm;
pub use google::protobuf as generated;

use alarm::{Status, status::State};
use chrono::{DateTime, TimeZone, Utc};
use generated::Timestamp;
use rust_pubsub_lib::{Publisher, Snapshot, Subscriber};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub mod phoebus;

#[cfg(test)]
mod tests;

/// The command that will come in from/should be sent to Phoebus during an acknowledgement.
pub const ACK_COMMAND: &str = "acknowledge";

/// Alias for the atomic cache of alarm state data, shared between the two synchronizing processes.
pub type AlarmStateCache = Arc<RwLock<HashMap<String, CachedState>>>;

/// Alias for the atomic cache of PV metadata shared between the two synchronizing processes.
pub type PvCache = Arc<RwLock<HashMap<String, phoebus::PvMetadata>>>;

/// Encapsulates the latest state information about an alarm.
#[derive(Clone, Debug, PartialEq)]
pub struct CachedState {
    /// The latest [`State`] the sync service has recorded for the alarm.
    pub state: State,
    /// If the alarm is snoozed, this field will be set to [`Some`] with the reenablement time, and
    /// the [`state`](Self::state) field will be set to [`Bypassed`](alarm::status::State::Bypassed).
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
        phoebus_host: String,
        phoebus_topics: Vec<String>,
    ) -> Self {
        SynchronizerConfig {
            alarm_states: Arc::new(RwLock::new(HashMap::<String, CachedState>::new())),
            cancel_token,
            controls_host,
            controls_topic,
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
        include!(concat!(env!("OUT_DIR"), "/common.alarm.rs"));
    }
}
mod google {
    pub mod protobuf {
        //! Generated Google Structs Module
        //!
        //! Contains the builtin structures (mainly [`Timestamp`]) provided by Google
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}
