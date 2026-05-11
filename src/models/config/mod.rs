//! Startup configuration and error types.
//!
//! Contains the types needed to initialize the synchronizer from environment variables
//! and to report failures during startup.

use std::collections::HashMap;
use std::sync::Arc;

use rust_pubsub_lib::{Publisher, Snapshot, Subscriber};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::models::AlarmStateCache;
use crate::models::cache::CachedState;
use crate::models::metadata::MetadataScope;

/// A trait to describe the basic functions of a synchronization process.
#[async_trait::async_trait]
pub trait Synchronizer<P: Publisher, S: Subscriber> {
    /// Constructs a [`Synchronizer`] instance from the provided [`SynchronizerConfig`] instance.
    fn new(config: SynchronizerConfig) -> Self;

    /// Kicks off the async process to monitor for alarm updates that need synchronization.
    async fn synchronize<SNAP: Snapshot>(self);
}

/// A narrower abstraction for synchronizers that run in the application's concrete Kafka runtime.
#[async_trait::async_trait]
pub trait RuntimeSyncFactory: Sized {
    /// Constructs a runtime synchronizer from the shared configuration.
    fn new(config: SynchronizerConfig) -> Self;

    /// Runs the synchronizer with the concrete Kafka publisher/subscriber/snapshot types used in production.
    async fn run(self);
}

/// Configuration data to initialize the synchronizer processes.
#[derive(Clone, Debug)]
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
    pub metadata_scope: MetadataScope,
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
            metadata_scope: MetadataScope::new(),
        }
    }
}

/// Manual implementation of [`PartialEq`] to compare [`alarm_states`](SynchronizerConfig::alarm_states)
/// by address instead of doing a deep comparison
impl PartialEq for SynchronizerConfig {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.alarm_states, &other.alarm_states)
            && self.controls_host == other.controls_host
            && self.controls_topic == other.controls_topic
            && self.grpc_alarms_svc_host == other.grpc_alarms_svc_host
            && self.phoebus_host == other.phoebus_host
            && self.phoebus_topics == other.phoebus_topics
            && self.metadata_scope == other.metadata_scope
    }
}

/// Error type for failures that occur while loading configuration from the environment.
#[derive(Debug, PartialEq)]
pub enum ConfigLoadError {
    /// A required environment variable was not set.
    MissingVariable(String),
}

impl std::fmt::Display for ConfigLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingVariable(var) => {
                write!(f, "Required environment variable '{var}' is not set")
            }
        }
    }
}

impl std::error::Error for ConfigLoadError {}

/// Error type for failures that occur while initializing the logging/tracing subscriber.
#[derive(Debug, PartialEq)]
pub enum LoggingInitError {
    /// A global tracing subscriber has already been set.
    AlreadyInitialized,
}

impl std::fmt::Display for LoggingInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInitialized => {
                write!(f, "A global tracing subscriber has already been set")
            }
        }
    }
}

impl std::error::Error for LoggingInitError {}
